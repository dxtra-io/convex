package convex.core.store;

import convex.core.data.ACell;
import convex.core.data.Hash;
import convex.core.data.Ref;
import convex.core.data.RefSoft;

/**
 * In-memory cache for Blob decoding. Should be used in the context of a specific Store
 */
public final class RefCache {

	private Ref<?>[] cache;
	private int size;
	
	private RefCache(int size) {
		this.size=size;
		this.cache=new Ref[size];
	};
	
	public static RefCache create(int size) {
		return new RefCache(size);
	}
	
	int getSize() {
		return size;
	}
	
	/**
	 * Gets the Cached Ref for a given hash, or null if not cached.
	 * @param hash Hash of Cell to look up in cache
	 * @return Cached Ref, or null if not found
	 */
	public Ref<?> getCell(Hash hash) {
		if (hash == null) return null; // Can't cache null hashes

		int ix=calcIndex(hash);
		Ref<?> ref=cache[ix];
		if (ref==null) return null;
		if (ref instanceof RefSoft) {
			if (!((RefSoft<?>)ref).hasReference()) {
				// Ref is missing, so kill in cache
				cache[ix]=null;
				return null;			
			}
		}
	
		if (ref.getHash().equals(hash)) return ref;
		return null; // different hash, hence not in cache
	}
	
	/**
	 * Stores a Ref in the cache
	 * @param cell Cell with Ref to store
	 */
	public void putCell(ACell cell) {
		Ref<?> ref=Ref.get(cell);
		putCell(ref);
	}
	
	/**
	 * Stores a Ref in the cache
	 * @param ref Ref to store
	 */
	public void putCell(Ref<?> ref) {
		Hash hash = ref.getHash();
		if (hash == null) return; // Can't cache refs with null hashes

		int ix=calcIndex(hash);
		cache[ix]=ref;
	}

	private int calcIndex(Hash h) {
		if (h == null) {
			// Handle null hash gracefully - use index 0
			// This can happen during store initialization when root hash is null
			return 0;
		}
		int hash=(int)h.longValue();
		int ix=Math.floorMod(hash, size);
		return ix;
	}


}
