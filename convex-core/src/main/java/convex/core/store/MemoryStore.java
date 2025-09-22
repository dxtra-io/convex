package convex.core.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import convex.core.data.ACell;
import convex.core.data.Blob;
import convex.core.data.Hash;
import convex.core.data.IRefFunction;
import convex.core.data.Ref;
import convex.core.exceptions.BadFormatException;
import convex.core.util.Utils;

/**
 * Class implementing direct in-memory caching and storage of hashed node data. 
 * 
 * Does not release storage unless closed or entire store is GC'd: mainly useful for testing
 * 
 * Persists refs as direct refs, i.e. retains fully in memory
 */
public class MemoryStore extends AStore {
	public static final MemoryStore DEFAULT = new MemoryStore();
	
	private static final Logger log = LoggerFactory.getLogger(MemoryStore.class.getName());

	/**
	 * Storage of persisted Refs for each hash value
	 */
	private final HashMap<Hash, Ref<ACell>> hashRefs = new HashMap<Hash, Ref<ACell>>();

	private ACell rootData;

	@Override
	@SuppressWarnings("unchecked")
	public <T extends ACell> Ref<T> refForHash(Hash hash) {
		Ref<T> ref = (Ref<T>) hashRefs.get(hash);
		if (ref!=null) return ref;
		if (hash==Hash.NULL_HASH) return (Ref<T>) Ref.NULL_VALUE;
		return null;
	}
	
	@Override
	public <T extends ACell> Ref<T> storeRef(Ref<T> r2, int status, Consumer<Ref<ACell>> noveltyHandler) throws IOException  {
		return storeRef(r2, status, noveltyHandler, false); 
	}

	@Override
	public <T extends ACell> Ref<T> storeTopRef(Ref<T> ref, int status,Consumer<Ref<ACell>> noveltyHandler) throws IOException  {
		return storeRef(ref,status, noveltyHandler, true); 
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public final <T extends ACell> T decode(Blob encoding) throws BadFormatException {
		Hash hash=encoding.getContentHash();
		Ref<?> cached= hashRefs.get(hash);
		if (cached!=null) return (T) cached.getValue();
		
		// Need to ensure we are reading with the current store set
		AStore tempStore=Stores.current();
		ACell decoded;
		if (tempStore==this) {
			decoded = decodeImpl(encoding);
		} else try {
			Stores.setCurrent(this);
			decoded = decodeImpl(encoding);
		} finally {
			Stores.setCurrent(tempStore);
		}
		return (T)decoded;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends ACell> Ref<T> storeRef(Ref<T> ref, int requiredStatus, Consumer<Ref<ACell>> noveltyHandler, boolean topLevel) throws IOException {
		// Convert to direct Ref. Don't want to store a soft ref!
		ref = ref.toDirect();

		final T o=ref.getValue();
		if (o==null) return (Ref<T>) Ref.NULL_VALUE;
		
		ACell cell = (ACell) o;
		boolean embedded=cell.isEmbedded();
		
		Hash hash=null;
		if (!embedded) {
			// check store for existing ref first. Return this is we have it
			hash = ref.getHash();
			Ref<T> existing = refForHash(hash);
			if ((existing != null)) {
				if (existing.getStatus()>=requiredStatus) return existing;
				ref=existing;
			}
		}
		if (requiredStatus < Ref.STORED) {
			return ref;
		}		
		// beyond STORED level, need to recursively persist child refs if they exist
		if ((requiredStatus > Ref.STORED) && (cell.getRefCount() > 0)) {
			// TODO: probably slow to rebuild these all the time!
			IRefFunction func = r -> {
				try {
					return storeRef((Ref<ACell>) r, requiredStatus, noveltyHandler, false);
				} catch (IOException e) {
					// OK because overall function throws IOException
					throw Utils.sneakyThrow(e);
				}
			};

			// need to do recursive persistence
			// TODO: maybe switch to a stack? Mitigate risk of stack overflow?
			ACell newObject = cell.updateRefs(func);

			// perhaps need to update Ref
			if (cell != newObject) {
				ref = ref.withValue((T) newObject);
				cell = newObject;
			}
		}


		/*
		// need to do recursive persistence
		cell  = cell.updateRefs(r -> {
			return storeRef(r,noveltyHandler,requiredStatus,false);
		});
		
		ref=ref.withValue((T)cell);

		*/
		final ACell oTemp=cell;
		if (topLevel || !embedded) {
			// Persist at top level
			final Hash fHash = (hash!=null)?hash:ref.getHash();
			if (log.isTraceEnabled()) {
				log.trace("Persisting ref 0x"+fHash.toHexString()+" of class "+Utils.getClassName(oTemp)+" with store "+this);
			}
			Ref<T> existing = refForHash(hash);

			ref = ref.withMinimumStatus(requiredStatus);
			hashRefs.put(fHash, (Ref<ACell>) ref);
			cell.attachRef(ref);

			// call novelty handler if newly persisted non-embedded
			if (noveltyHandler != null && existing == null) {
				if (!embedded)
					noveltyHandler.accept((Ref<ACell>) ref);
			}
		}
		else {
			ref = ref.withMinimumStatus(requiredStatus);
		}
		cell.attachRef(ref);
		return ref;
	}

	@Override
	public Hash getRootHash() throws IOException {
		return rootData == null ? null : rootData.getHash();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends ACell> T getRootData() throws IOException {
		return (T) rootData;
	}

	@Override
	public <T extends ACell> Ref<T> setRootData(T data) {
		rootData=data;
		return Ref.get(data);
	}

	@Override
	public void close() {
		hashRefs.clear();
		rootData=null;
	}

	@Override
	public <T extends ACell> Ref<T> checkCache(Hash h) {
		return refForHash(h);
	}

	@Override
	public String shortName() {
		return "Memory Store "+Objects.toIdentityString(this);
	}
}
