package convex.cli.mixins;

import convex.cli.CLIError;
import convex.cli.ExitCodes;
import convex.postgres.PostgresSchemaManager;
import convex.postgres.PostgresStore;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

import java.sql.SQLException;

/**
 * CLI mixin for configuring connections to a PostgreSQL-backed Convex store.
 */
public class PostgresMixin extends AMixin {

    @Option(names = "--pg-url",
            scope = ScopeType.INHERIT,
            description = "JDBC URL for PostgreSQL connection. Overrides individual host/port options. Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:CONVEX_PG_URL:-jdbc:postgresql://localhost:5432/convex}")
    protected String jdbcUrl;

    @Option(names = "--pg-user",
            scope = ScopeType.INHERIT,
            description = "PostgreSQL user. Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:POSTGRES_USER:-convex}")
    protected String user;

    @Option(names = "--pg-password",
            scope = ScopeType.INHERIT,
            description = "PostgreSQL password. Default: ${DEFAULT-VALUE}",
            defaultValue = "${env:POSTGRES_PASSWORD:-convex}")
    protected String password;

    @Option(names = "--pg-reset",
            scope = ScopeType.INHERIT,
            description = "Drop and recreate Convex schema before use. Default: ${DEFAULT-VALUE}")
    protected boolean resetSchema;

    /**
     * Opens a {@link PostgresStore} based on the configured connection options.
     * Callers are responsible for closing the returned store.
     */
    public PostgresStore openStore() {
        return openStore(false);
    }

    public PostgresStore openStore(boolean forceReset) {
        boolean reset = resetSchema || forceReset;
        try {
            PostgresSchemaManager.ensureSchema(jdbcUrl, user, password, reset);
            PostgresStore store = new PostgresStore(jdbcUrl, user, password);
            return store;
        } catch (SQLException e) {
            throw new CLIError(ExitCodes.IOERR, "Failed to initialise PostgreSQL schema: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new CLIError(ExitCodes.IOERR, "Unable to connect to PostgreSQL store: " + e.getMessage(), e);
        }
    }
}
