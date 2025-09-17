package convex.core.store;

public class Stores {

	// Configured global store
	private static AStore globalStore=null;

	// Thread local current store, in case servers want different stores
	private static final ThreadLocal<AStore> currentStore = new ThreadLocal<>() {
		@Override
		protected AStore initialValue() {
			return getGlobalStore();
		}
	};

	/**
	 * Gets the current (thread-local) Store instance. This is initialised to be the
	 * global store, but can be changed with Stores.setCurrent(...)
	 *
	 * @return Store for the current thread
	 */
	public static AStore current() {
		return Stores.currentStore.get();
	}

	/**
	 * Sets the current thread-local store for this thread
	 *
	 * @param store Any AStore instance
	 */
	public static void setCurrent(AStore store) {
		currentStore.set(store);
	}

	/**
	 * Gets the global store instance. PostgreSQL store must be explicitly configured.
	 *
	 * @return Current global store
	 * @throws IllegalStateException if no global store has been set
	 */
	public static AStore getGlobalStore() {
		if (globalStore==null) {
			throw new IllegalStateException("No global store configured. PostgreSQL store must be explicitly set via setGlobalStore().");
		}
		return globalStore;
	}

	/**
	 * Sets the global store for this JVM. Global store is the store used for
	 * any new thread.
	 *
	 * @param store Store instance to use as global store
	 */
	public static void setGlobalStore(AStore store) {
		if (store==null) throw new IllegalArgumentException("Cannot set global store to null");
		globalStore=store;
	}
}
