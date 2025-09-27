package convex.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import convex.core.store.AStore;
import convex.core.store.Stores;
import convex.postgres.PostgresStore;

/**
 * Base class for tests that require a configured global {@link Stores} instance.
 *
 * <p>The default implementation uses a shared {@link PostgresStore} provided by
 * {@link PostgresTestHelper}, backed by a Testcontainers-managed PostgreSQL instance.</p>
 */
public abstract class BaseStoreTest {

    protected static AStore store;

    @BeforeAll
    static synchronized void setUpGlobalStore() {
        if (store == null) {
            store = PostgresTestHelper.ensureStore();
            Stores.setGlobalStore(store);
        } else {
            Stores.setGlobalStore(store);
        }
        Stores.setCurrent(store);
    }

    @AfterEach
    void resetCurrentStore() {
        Stores.setCurrent(store);
    }
}
