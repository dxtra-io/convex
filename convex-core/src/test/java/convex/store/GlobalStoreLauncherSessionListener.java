package convex.store;

import org.junit.platform.launcher.LauncherSession;
import org.junit.platform.launcher.LauncherSessionListener;

import convex.core.store.AStore;
import convex.core.store.Stores;

/**
 * Ensures a global store is configured before any tests are executed.
 */
public final class GlobalStoreLauncherSessionListener implements LauncherSessionListener {

    @Override
    public void launcherSessionOpened(LauncherSession session) {
        AStore store = PostgresTestHelper.ensureStore();
        Stores.setGlobalStore(store);
        Stores.setCurrent(store);
    }
}
