package convex.postgres;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private volatile Hash rootHash = null;

    // Concurrency control for cell operations
    private final ConcurrentHashMap<Hash, Object> cellLocks = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock storeLock = new ReentrantReadWriteLock(true);

    // Track cells currently being stored to prevent duplicate operations
    private final ConcurrentHashMap<Hash, Boolean> cellsBeingStored = new ConcurrentHashMap<>();

    // SQL statements
    private static final String INSERT_CELL =
        "INSERT INTO convex.cells (hash, encoding, status, size) VALUES (?, ?, ?, ?) ON CONFLICT (hash) DO NOTHING";
    private static final String SELECT_CELL =
        "SELECT encoding FROM convex.cells WHERE hash = ?";
    private static final String SELECT_CELL_EXISTS =
        "SELECT 1 FROM convex.cells WHERE hash = ? LIMIT 1";
    private static final String UPSERT_CELL =
        "INSERT INTO convex.cells (hash, encoding, status, size) VALUES (?, ?, ?, ?) " +
        "ON CONFLICT (hash) DO UPDATE SET status = GREATEST(convex.cells.status, EXCLUDED.status)";
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
            this.rootHash = null;
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

        // Connection pool configuration optimized for high concurrency
        config.setMaximumPoolSize(25);
        config.setMinimumIdle(5);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        config.setLeakDetectionThreshold(60000);

        // PostgreSQL specific settings for concurrency
        Properties props = new Properties();
        props.setProperty("preparedStatementCacheQueries", "256");
        props.setProperty("preparedStatementCacheSizeMiB", "5");
        props.setProperty("reWriteBatchedInserts", "true");
        props.setProperty("defaultTransactionIsolation", "TRANSACTION_READ_COMMITTED");
        props.setProperty("readOnlyMode", "false");
        props.setProperty("autoCommit", "true");
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
                    this.rootHash = null;
                }
            } else {
                this.rootHash = null;
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

            // Use read lock for cache check to prevent interference with concurrent stores
            storeLock.readLock().lock();
            try {
                Ref<T> existing = refForHash(hash);
                if (existing != null && existing.getStatus() >= requiredStatus) {
                    log.debug("existing {}", hash);
                    return existing;
                }
            } finally {
                storeLock.readLock().unlock();
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

            // Use atomic store operation with proper locking
            ref = storeRefAtomic(ref, fHash, cell, requiredStatus, noveltyHandler, embedded);

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

    /**
     * Atomic store operation with proper concurrency control
     */
    @SuppressWarnings("unchecked")
    private <T extends ACell> Ref<T> storeRefAtomic(Ref<T> ref, Hash hash, ACell cell, int requiredStatus,
                                                    Consumer<Ref<ACell>> noveltyHandler, boolean embedded) throws IOException {
        // Get per-hash lock to prevent concurrent operations on same cell
        Object lockObject = cellLocks.computeIfAbsent(hash, k -> new Object());

        synchronized (lockObject) {
            try {
                // Check if another thread is already storing this cell
                if (cellsBeingStored.putIfAbsent(hash, Boolean.TRUE) != null) {
                    // Another thread is storing this cell, wait and then check cache
                    Thread.yield();
                    Ref<T> existing = refForHash(hash);
                    if (existing != null && existing.getStatus() >= requiredStatus) {
                        return existing;
                    }
                }

                // Double-check cache under lock
                Ref<T> existing = refForHash(hash);
                if (existing != null && existing.getStatus() >= requiredStatus) {
                    return existing;
                }

                // Atomic database operation - upsert with status update
                boolean wasNovel = storeCellAtomically(hash, cell, requiredStatus);

                // Update ref status and cache atomically
                storeLock.writeLock().lock();
                try {
                    ref = ref.withMinimumStatus(requiredStatus);
                    if (!embedded) {
                        ref = ref.toSoft(this);
                    }
                    cell.attachRef(ref);
                    refCache.putCell(ref);
                } finally {
                    storeLock.writeLock().unlock();
                }

                // Call novelty handler if provided and cell was novel
                if (noveltyHandler != null && wasNovel && !embedded) {
                    noveltyHandler.accept((Ref<ACell>) ref);
                }

                return ref;

            } finally {
                // Always remove from tracking map
                cellsBeingStored.remove(hash);
            }
        }
    }

    /**
     * Atomic database operation to store or update cell
     * @return true if this was a novel cell (first time stored)
     */
    private boolean storeCellAtomically(Hash hash, ACell cell, int status) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Encode the cell for storage
                Blob encoding = Format.encodedBlob(cell);

                // Use INSERT with ON CONFLICT to detect if cell is novel
                boolean wasNovel;
                try (PreparedStatement stmt = conn.prepareStatement(INSERT_CELL)) {
                    stmt.setString(1, hash.toHexString());
                    stmt.setBytes(2, encoding.getBytes());
                    stmt.setInt(3, status);
                    stmt.setInt(4, encoding.size());
                    log.debug("storeCellAtomically: {}", stmt.toString());
                    int rowsAffected = stmt.executeUpdate();

                    // If rowsAffected > 0, the cell was inserted (novel)
                    // If rowsAffected = 0, the cell already existed (conflict, not novel)
                    wasNovel = rowsAffected > 0;
                }

                // If not novel but status might need updating, use UPDATE
                if (!wasNovel) {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "UPDATE convex.cells SET status = GREATEST(status, ?) WHERE hash = ?")) {
                        stmt.setInt(1, status);
                        stmt.setString(2, hash.toHexString());
                        stmt.executeUpdate();
                    }
                }

                conn.commit();
                return wasNovel;

            } catch (SQLException e) {
                conn.rollback();
                throw new IOException("Atomic cell storage failed for hash: " + hash.toHexString(), e);
            }
        } catch (SQLException e) {
            throw new IOException("Database connection failed during atomic store", e);
        }
    }

    private boolean cellExistsInDatabase(Hash hash) throws IOException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CELL_EXISTS)) {

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
        // Handle null hash case
        if (hash == null) return null;

        // Handle special cases
        if (hash.equals(Hash.NULL_HASH)) {
            return (Ref<T>) Ref.NULL_VALUE;
        }

        // Use read lock for consistent cache access
        storeLock.readLock().lock();
        try {
            // Check cache first
            Ref<T> cached = (Ref<T>) refCache.getCell(hash);
            if (cached != null) return cached;
        } finally {
            storeLock.readLock().unlock();
        }

        // Load from PostgreSQL with proper error handling and retries
        return loadCellFromDatabase(hash);
    }

    /**
     * Load cell from database with proper error handling
     */
    @SuppressWarnings("unchecked")
    private <T extends ACell> Ref<T> loadCellFromDatabase(Hash hash) {
        final int maxRetries = 3;
        final long retryDelayMs = 10;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
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

                    // Cache for future access atomically
                    storeLock.writeLock().lock();
                    try {
                        refCache.putCell(ref);
                    } finally {
                        storeLock.writeLock().unlock();
                    }

                    return ref;
                }

                // Cell not found in database
                return null;

            } catch (SQLException e) {
                log.warn("Database error loading cell (attempt {} of {}): {}",
                        attempt + 1, maxRetries, hash.toHexString(), e);

                if (attempt == maxRetries - 1) {
                    log.error("Failed to load cell after {} attempts: {}", maxRetries, hash.toHexString());
                    return null;
                }

                // Brief retry delay
                try {
                    Thread.sleep(retryDelayMs * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }

            } catch (BadFormatException e) {
                log.error("Data corruption detected for hash: {}", hash.toHexString(), e);
                return null;
            }
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

        // Update root hash in database with transaction
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement stmt = conn.prepareStatement(SET_ROOT_HASH)) {
                    stmt.setString(1, newRootHash.toHexString());
                    log.debug("setRootData: {}", stmt.toString());
                    int rowsAffected = stmt.executeUpdate();

                    if (rowsAffected == 0) {
                        throw new IOException("Failed to update root hash - no rows affected");
                    }
                }
                conn.commit();

                // Update cached root hash only after successful database update
                synchronized (this) {
                    this.rootHash = newRootHash;
                }

                log.debug("Set new root hash: {}", newRootHash.toHexString());

            } catch (SQLException e) {
                conn.rollback();
                throw new IOException("Failed to update root hash in transaction", e);
            }
        } catch (SQLException e) {
            throw new IOException("Database connection failed during root update", e);
        }

        return ref;
    }

    public <T extends ACell> Ref<T> readStoreRef(Hash hash) throws IOException {
        // First check cache under read lock
        storeLock.readLock().lock();
        try {
            @SuppressWarnings("unchecked")
            Ref<T> cached = (Ref<T>) refCache.getCell(hash);
            if (cached != null && cached.getStatus() >= Ref.STORED) {
                return cached;
            }
        } finally {
            storeLock.readLock().unlock();
        }

        // Load from database with proper error handling
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(SELECT_CELL)) {

            stmt.setString(1, hash.toHexString());
            log.debug("readStoreRef: {} ", stmt.toString());
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                byte[] encoding = rs.getBytes("encoding");
                Blob blob = Blob.wrap(encoding);

                ACell cell = decode(blob);
                @SuppressWarnings("unchecked")
                Ref<T> ref = (Ref<T>) Ref.get(cell);
                ref = ref.withMinimumStatus(Ref.STORED);
                ref = ref.toSoft(this);

                // Cache atomically
                storeLock.writeLock().lock();
                try {
                    refCache.putCell(ref);
                } finally {
                    storeLock.writeLock().unlock();
                }

                return ref;
            }

        } catch (SQLException | BadFormatException e) {
            throw new IOException("Failed to read store ref for hash: " + hash.toHexString(), e);
        }

        return null;
    }

    @Override
    public void close() {
        storeLock.writeLock().lock();
        try {
            if (dataSource != null && !dataSource.isClosed()) {
                // Clear tracking maps
                cellsBeingStored.clear();
                cellLocks.clear();

                dataSource.close();
                log.info("PostgresStore closed");
            }
        } finally {
            storeLock.writeLock().unlock();
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

    /**
     * Gets the underlying DataSource for transaction metadata operations
     * This is required for the transaction prepare/submit workflow
     * @return The HikariDataSource instance used by this store
     */
    public DataSource getDataSource() {
        return dataSource;
    }
}