package convex.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for creating and managing the Convex PostgreSQL schema.
 */
public final class PostgresSchemaManager {

    private static final Logger log = LoggerFactory.getLogger(PostgresSchemaManager.class);

    private PostgresSchemaManager() {
    }

    public static void ensureSchema(PostgresStore store, boolean reset) throws SQLException {
        DataSource dataSource = store.getDataSource();
        try (Connection conn = dataSource.getConnection()) {
            ensureSchema(conn, reset, store.shortName());
        }
        if (reset) {
            store.reloadRootHash();
        }
    }

    public static void ensureSchema(String jdbcUrl, String username, String password, boolean reset) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            ensureSchema(conn, reset, jdbcUrl);
        }
    }

    private static void ensureSchema(Connection conn, boolean reset, String context) throws SQLException {
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            if (reset) {
                log.info("Resetting convex schema in PostgreSQL store {}", context);
                stmt.execute("DROP SCHEMA IF EXISTS convex CASCADE");
            }

            stmt.execute("CREATE SCHEMA IF NOT EXISTS convex");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS convex.cells (
                    hash VARCHAR(64) PRIMARY KEY,
                    encoding BYTEA NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0,
                    size INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS convex.root (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    root_hash VARCHAR(64),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT single_root CHECK (id = 1)
                )
            """);

            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cells_hash ON convex.cells USING HASH(hash)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cells_status ON convex.cells(status)");

            stmt.execute("INSERT INTO convex.root (id, root_hash) VALUES (1, NULL) ON CONFLICT (id) DO NOTHING");
        }
    }
}
