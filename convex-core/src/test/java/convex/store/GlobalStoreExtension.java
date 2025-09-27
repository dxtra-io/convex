package convex.store;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import convex.core.store.AStore;
import convex.core.store.Stores;

/**
 * JUnit Jupiter extension ensuring each test class has a configured global store.
 */
public final class GlobalStoreExtension implements BeforeAllCallback, AfterEachCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        AStore store = PostgresTestHelper.ensureStore();
        Stores.setGlobalStore(store);
        Stores.setCurrent(store);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        AStore store = PostgresTestHelper.ensureStore();
        Stores.setCurrent(store);
    }
}
