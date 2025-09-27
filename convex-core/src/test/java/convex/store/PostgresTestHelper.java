package convex.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import convex.core.store.Stores;
import convex.postgres.PostgresStore;

/**
 * Shared PostgreSQL Testcontainer lifecycle that provides a {@link PostgresStore}
 * for tests that need persistence backed by a real database.
 */
public final class PostgresTestHelper {

    private static final Logger log = LoggerFactory.getLogger(PostgresTestHelper.class);

    private static final String IMAGE = System.getProperty(
            "convex.test.postgres.image", "postgres:15.8-alpine");

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(IMAGE)
            .withDatabaseName("convex_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    private static volatile PostgresStore sharedStore;
    private static volatile boolean schemaInitialised = false;

    private PostgresTestHelper() {
        // utility
    }

    public static synchronized PostgresStore ensureStore() {
        if (sharedStore != null) {
            return sharedStore;
        }

        startContainerIfNeeded();
        initialiseSchema();

        sharedStore = new PostgresStore(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword());

        Stores.setGlobalStore(sharedStore);
        Stores.setCurrent(sharedStore);

        log.info("PostgresStore test instance initialised at {}", POSTGRES.getJdbcUrl());
        return sharedStore;
    }

    static synchronized void ensureCurrentSchema() {
        startContainerIfNeeded();
        initialiseSchema();
    }

    private static void startContainerIfNeeded() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
    }

    private static void initialiseSchema() {
        if (schemaInitialised) {
            return;
        }

        try (Connection conn = POSTGRES.createConnection("");
             Statement stmt = conn.createStatement()) {

            stmt.execute("DROP SCHEMA IF EXISTS convex CASCADE");
            stmt.execute("CREATE SCHEMA convex");

            stmt.execute("""
                CREATE TABLE convex.cells (
                    hash VARCHAR(64) PRIMARY KEY,
                    encoding BYTEA NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0,
                    size INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """);

            stmt.execute("""
                CREATE TABLE convex.root (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    root_hash VARCHAR(64),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT single_root CHECK (id = 1)
                )
            """);

            stmt.execute("INSERT INTO convex.root (id, root_hash) VALUES (1, NULL)");

            stmt.execute("CREATE INDEX idx_cells_hash ON convex.cells USING HASH(hash)");
            stmt.execute("CREATE INDEX idx_cells_status ON convex.cells(status)");

            schemaInitialised = true;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialise Postgres schema for tests", e);
        }
    }

    private static synchronized void shutdown() {
        if (sharedStore != null) {
            sharedStore.close();
            sharedStore = null;
        }
        if (POSTGRES.isRunning()) {
            POSTGRES.stop();
        }
        schemaInitialised = false;
    }
}
