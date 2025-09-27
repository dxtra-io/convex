package convex.cli;

import convex.postgres.PostgresSchemaManager;
import convex.postgres.PostgresStore;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.SQLException;

public final class PostgresTestHelper {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15.8-alpine")
            .withDatabaseName("convex_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    private PostgresTestHelper() {
    }

    public static synchronized PostgresStore createStore(boolean reset) {
        ensureStarted();
        try {
            PostgresSchemaManager.ensureSchema(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(), reset);
            return new PostgresStore(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialise schema for test", e);
        }
    }

    public static synchronized String jdbcUrl() {
        ensureStarted();
        return POSTGRES.getJdbcUrl();
    }

    public static synchronized String username() {
        ensureStarted();
        return POSTGRES.getUsername();
    }

    public static synchronized String password() {
        ensureStarted();
        return POSTGRES.getPassword();
    }

    private static void ensureStarted() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
    }

}
