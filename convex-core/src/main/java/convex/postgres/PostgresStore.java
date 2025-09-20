package convex.postgres;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import convex.core.data.ACell;
import convex.core.data.Blob;
import convex.core.data.Format;
import convex.core.data.Hash;
import convex.core.data.IRefFunction;
import convex.core.data.Ref;
import convex.core.exceptions.BadFormatException;
import convex.core.store.ACachedStore;
import convex.core.util.Utils;

/**
 * PostgreSQL-based implementation of Convex storage using content-addressed storage.
 *
 * This store provides an alternative to EtchStore for persistent storage of Convex Cells,
 * using PostgreSQL as the underlying storage mechanism with content-addressed hashing.
 *
 * Schema:
 * - convex.cells: stores encoded cell data by hash
 * - convex.root: stores the current root hash
 */
public class PostgresStore extends ACachedStore {
    private static final Logger log = LoggerFactory.getLogger(PostgresStore.class.getName());

    private final HikariDataSource dataSource;
    private volatile Hash rootHash = Hash.NULL_HASH;

    // SQL statements
    private static final String INSERT_CELL =
        "INSERT INTO convex.cells (hash, encoding, status, size) VALUES (?, ?, ?, ?) ON CONFLICT (hash) DO NOTHING";
    private static final String SELECT_CELL =
        "SELECT encoding FROM convex.cells WHERE hash = ?";
    private static final String GET_ROOT_HASH =
        "SELECT root_hash FROM convex.root WHERE id = 1";
    private static final String SET_ROOT_HASH =
        "INSERT INTO convex.root (id, root_hash, updated_at) VALUES (1, ?, NOW()) " +
        "ON CONFLICT (id) DO UPDATE SET root_hash = EXCLUDED.root_hash, updated_at = NOW()";

    /**
     * Creates a PostgresStore with the given configuration
     */
    public PostgresStore(String jdbcUrl, String username, String password) {
        this(createDataSource(jdbcUrl, username, password));
    }

    /**
     * Creates a PostgresStore with an existing DataSource
     */
    public PostgresStore(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource) {
            this.dataSource = (HikariDataSource) dataSource;
        } else {
            throw new IllegalArgumentException("DataSource must be HikariDataSource");
        }

        // Initialize root hash from database
        try {
            initializeRootHash();
        } catch (IOException e) {
            log.warn("Failed to initialize root hash from database", e);
            this.rootHash = Hash.NULL_HASH;
        }

        log.info("PostgresStore initialized with JDBC URL: {}",
                 this.dataSource.getJdbcUrl());
    }

    /**
     * Creates a properly configured HikariDataSource for PostgreSQL
     */
    private static HikariDataSource createDataSource(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName("org.postgresql.Driver");

        // Connection pool configuration optimized for Convex
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        // PostgreSQL specific settings
        Properties props = new Properties();
        props.setProperty("preparedStatementCacheQueries", "256");
        props.setProperty("preparedStatementCacheSizeMiB", "5");
        props.setProperty("reWriteBatchedInserts", "true");
        config.setDataSourceProperties(props);

        return new HikariDataSource(config);
    }

    /**
     * Create PostgresStore from environment variables
     */
    public static PostgresStore fromEnvironment() {
        String host = System.getenv().getOrDefault("POSTGRES_HOST", "localhost");
        String port = System.getenv().getOrDefault("POSTGRES_PORT", "5432");
        String database = System.getenv().getOrDefault("POSTGRES_DATABASE", "postgres");
        String username = System.getenv().getOrDefault("POSTGRES_USER", "postgres");
        String password = System.getenv().getOrDefault("POSTGRES_PASSWORD", "postgres");
        String sslMode = System.getenv().getOrDefault("POSTGRES_SSL_MODE", "prefer");

        String jdbcUrl = String.format("jdbc:postgresql://%s:%s/%s?sslmode=%s",
                                     host, port, database, sslMode);

        return new PostgresStore(jdbcUrl, username, password);
    }

    private void initializeRootHash() throws IOException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(GET_ROOT_HASH)) {

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                String hashStr = rs.getString("root_hash");
                if (hashStr != null && !hashStr.isEmpty()) {
                    this.rootHash = Hash.fromHex(hashStr);
                } else {
                    this.rootHash = Hash.NULL_HASH;
                }
            } else {
                this.rootHash = Hash.NULL_HASH;
            }

        } catch (SQLException e) {
            throw new IOException("Failed to initialize root hash", e);
        }
    }

    @Override
    public <T extends ACell> Ref<T> storeRef(Ref<T> ref, int status, Consumer<Ref<ACell>> noveltyHandler) throws IOException {
        return storeRef(ref, status, noveltyHandler, false);
    }

    @Override
    public <T extends ACell> Ref<T> storeTopRef(Ref<T> ref, int status, Consumer<Ref<ACell>> noveltyHandler) throws IOException {
        return storeRef(ref, status, noveltyHandler, true);
    }

    @SuppressWarnings("unchecked")
    private <T extends ACell> Ref<T> storeRef(Ref<T> ref, int requiredStatus, Consumer<Ref<ACell>> noveltyHandler, boolean topLevel) throws IOException {
        // Get the value
        ACell cell = ref.getValue();

        // Handle null case
        if (cell == null) return (Ref<T>) Ref.NULL_VALUE;

        // Check if already in cache with sufficient status
        boolean embedded = cell.isEmbedded();
        Hash hash = null;

        if (!embedded) {
            hash = ref.getHash();
            Ref<T> existing = refForHash(hash);
            if (existing != null && existing.getStatus() >= requiredStatus) {
                log.debug("existing {}", hash);
                return existing;
            }
        }

        // For status < STORED, just update cache
        if (requiredStatus < Ref.STORED) {
            if (topLevel || !embedded) {
                refCache.putCell(ref);
            }
            log.debug("add to cache ()", hash);
            return ref;
        }

		// beyond STORED level, need to recursively persist child refs if they exist
		if ((requiredStatus > Ref.STORED) && (cell.getRefCount() > 0)) {
			// TODO: probably slow to rebuild these all the time!
			IRefFunction func = r -> {
                try {
                    return storeRef((Ref<ACell>) r, requiredStatus, noveltyHandler, false);
                } catch (IOException e) {
                    // OK because overall function throws IOException
                    throw Utils.sneakyThrow(e);
                }
            };

			// need to do recursive persistence
			// TODO: maybe switch to a stack? Mitigate risk of stack overflow?
			ACell newObject = cell.updateRefs(func);

			// perhaps need to update Ref
			if (cell != newObject) {
				ref = ref.withValue((T) newObject);
				cell = newObject;
			}
		}

        // Store to PostgreSQL if not embedded or if top level
        if (topLevel || !embedded) {
            final Hash fHash = (hash != null) ? hash : ref.getHash();

            // Check if already exists in database
            if (!cellExistsInDatabase(fHash)) {
                // Encode the cell for storage
                Blob encoding = Format.encodedBlob(cell);

                // Store in PostgreSQL
                storeCellToDatabase(fHash, encoding, requiredStatus);
            }

            // Update ref status and cache
            ref = ref.withMinimumStatus(requiredStatus);
            if (!embedded) {
                ref = ref.toSoft(this);
            }
            cell.attachRef(ref);
            refCache.putCell(ref);
            // Call novelty handler if provided for new data
            if (noveltyHandler != null) {
                if (!embedded) {
                    noveltyHandler.accept((Ref<ACell>) ref);
                }
            }
            log.trace("Stored cell with hash: {}", fHash.toHexString());
        }
        else {
            // For embedded values, don't store unless top level
            log.debug("not top level {}", ref.getHash());
            return ref.withMinimumStatus(requiredStatus);
        }

        cell.attachRef(ref);
        return ref;
    }

    private boolean cellExistsInDatabase(Hash hash) throws IOException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CELL)) {

            stmt.setString(1, hash.toHexString());
            log.debug("cellExistsInDatabase: {}", stmt.toString());
            ResultSet rs = stmt.executeQuery();
            return rs.next();

        } catch (SQLException e) {
            throw new IOException("Failed to check cell existence", e);
        }
    }

    private void storeCellToDatabase(Hash hash, Blob encoding, int status) throws IOException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(INSERT_CELL)) {

            stmt.setString(1, hash.toHexString());
            stmt.setBytes(2, encoding.getBytes());
            stmt.setInt(3, status);
            stmt.setInt(4, encoding.size());
            log.debug("storeCellToDatabase: {}", stmt.toString());
            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new IOException("Failed to store cell", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends ACell> Ref<T> refForHash(Hash hash) {
        // Check cache first
        Ref<T> cached = (Ref<T>) refCache.getCell(hash);
        if (cached != null) return cached;

        // Handle special cases
        if (hash.equals(Hash.NULL_HASH)) {
            return (Ref<T>) Ref.NULL_VALUE;
        }

        // Load from PostgreSQL
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CELL)) {

            stmt.setString(1, hash.toHexString());
            ResultSet rs = stmt.executeQuery();

            log.debug("refForHash: {}", stmt.toString());
            if (rs.next()) {
                byte[] encoding = rs.getBytes("encoding");
                Blob blob = Blob.wrap(encoding);

                // Decode the cell
                ACell cell = decode(blob);
                Ref<T> ref = (Ref<T>) Ref.get(cell);
                ref = ref.withMinimumStatus(Ref.STORED);
                ref = ref.toSoft(this);

                // Cache for future access
                refCache.putCell(ref);

                return ref;
            }

        } catch (SQLException | BadFormatException e) {
            log.warn("Failed to load cell for hash: {}", hash.toHexString(), e);
        }

        return null;
    }

    @Override
    public Hash getRootHash() throws IOException {
        return rootHash;
    }

    @Override
    public <T extends ACell> Ref<T> setRootData(T data) throws IOException {
        // Store the data first
        Ref<T> ref = storeTopRef(Ref.get(data), Ref.PERSISTED, null);
        Hash newRootHash = Hash.get(data);

        // Update root hash in database
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SET_ROOT_HASH)) {

            stmt.setString(1, newRootHash.toHexString());
            log.debug("setRootData: {}", stmt.toString());
            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new IOException("Failed to update root hash", e);
        }

        // Update cached root hash
        this.rootHash = newRootHash;

        log.debug("Set new root hash: {}", newRootHash.toHexString());
        return ref;
    }

    public <T extends ACell> Ref<T> readStoreRef(Hash hash) throws IOException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CELL)) {

            stmt.setString(1, hash.toHexString());
            log.debug("readStoreRef: {} ", stmt.toString());
            ResultSet rs = stmt.executeQuery();


            if (rs.next()) {
                byte[] encoding = rs.getBytes("encoding");
                Blob blob = Blob.wrap(encoding);

                ACell cell = decode(blob);
                Ref<T> ref = (Ref<T>) Ref.get(cell);
                ref = ref.withMinimumStatus(Ref.STORED);
                ref = ref.toSoft(this);

                refCache.putCell(ref);
                return ref;
            }

        } catch (SQLException | BadFormatException e) {
            throw new IOException("Failed to read store ref", e);
        }

        return null;
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("PostgresStore closed");
        }
    }

    @Override
    public String shortName() {
        return "Postgres: " + dataSource.getJdbcUrl();
    }

    /**
     * Get connection pool statistics
     */
    public String getPoolStats() {
        return String.format("Pool[active=%d, idle=%d, total=%d]",
                           dataSource.getHikariPoolMXBean().getActiveConnections(),
                           dataSource.getHikariPoolMXBean().getIdleConnections(),
                           dataSource.getHikariPoolMXBean().getTotalConnections());
    }
}