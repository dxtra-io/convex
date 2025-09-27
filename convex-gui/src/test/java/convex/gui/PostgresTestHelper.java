package convex.gui;

import convex.postgres.PostgresSchemaManager;
import convex.postgres.PostgresStore;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;

final class PostgresTestHelper {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15.8-alpine")
            .withDatabaseName("convex_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    private PostgresTestHelper() {
    }

    static synchronized PostgresStore createStore(boolean resetSchema) {
        ensureStarted();
        try {
            PostgresSchemaManager.ensureSchema(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(), resetSchema);
            return new PostgresStore(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialise PostgreSQL schema for GUI tests", e);
        }
    }

    private static void ensureStarted() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
    }

}
