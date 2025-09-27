package convex.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import convex.core.Belief;
import convex.core.Block;
import convex.core.Order;
import convex.core.crypto.AKeyPair;
import convex.core.data.ACell;
import convex.core.data.AMap;
import convex.core.data.AVector;
import convex.core.data.Blob;
import convex.core.data.Blobs;
import convex.core.data.Cells;
import convex.core.data.Format;
import convex.core.data.Hash;
import convex.core.data.Keywords;
import convex.core.data.Lists;
import convex.core.data.Keyword;
import convex.core.data.Maps;
import convex.core.data.Ref;
import convex.core.data.Refs;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.core.transactions.ATransaction;
import convex.core.transactions.Invoke;
import convex.core.transactions.Transfer;
import convex.core.init.InitTest;
import convex.core.lang.Symbols;
import convex.core.store.AStore;
import convex.core.store.Stores;
import convex.core.store.MemoryStore;
import convex.core.util.Utils;

// EtchStore removed - PostgreSQL only

/**
 * Comprehensive tests for PostgresStore using TestContainers
 */
import org.junit.jupiter.api.Disabled;

@Disabled
@Testcontainers
public class PostgresStoreTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15.8-alpine")
            .withDatabaseName("convex_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    private PostgresStore store;

    @BeforeEach
    void setUp() throws SQLException {
        // Create schema before each test
        createSchema();

        // Create store instance
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        store = new PostgresStore(jdbcUrl, username, password);

        // Initialize global store to prevent IllegalStateException
        try {
            Stores.setGlobalStore(store);
            Stores.setCurrent(store);
        } catch (Exception e) {
            // If this fails, continue with test
        }
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
        // Reset global store to default to prevent test interference
        try {
            MemoryStore memoryStore = new MemoryStore();
            Stores.setGlobalStore(memoryStore);
            Stores.setCurrent(memoryStore);

        } catch (Exception e) {
            // Ignore reset errors, other tests will create their own stores
        }
    }

    private void createSchema() throws SQLException {
        try (Connection conn = postgres.createConnection("");
             Statement stmt = conn.createStatement()) {

            // Create schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS convex");

            // stmt.execute("DROP TABLE IF EXISTS convex.cells");
            // stmt.execute("DROP TABLE IF EXISTS convex.root");

            // Create cells table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS convex.cells (
                    hash VARCHAR(64) PRIMARY KEY,
                    encoding BYTEA NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0,
                    size INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """);

            // Create root table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS convex.root (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    root_hash VARCHAR(64),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT single_root CHECK (id = 1)
                )
            """);

            // Insert initial root
            stmt.execute("INSERT INTO convex.root (id, root_hash) VALUES (1, NULL) ON CONFLICT (id) DO NOTHING");

            // Create indexes
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cells_hash ON convex.cells USING HASH(hash)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cells_status ON convex.cells(status)");
        }
    }

    @Test
    void testBasicStoreAndRetrieve() throws IOException {
        // Test basic string storage
        String testData = "Hello, PostgresStore!";
        Ref<convex.core.data.AString> ref = store.storeTopRef(Ref.get(Strings.create(testData)), Ref.STORED, null);

        assertNotNull(ref);
        assertEquals(Ref.STORED, ref.getStatus());

        // Retrieve by hash
        Hash hash = ref.getHash();
        Ref<convex.core.data.AString> retrieved = store.refForHash(hash);

        assertNotNull(retrieved);
        assertEquals(testData, retrieved.getValue().toString());
    }

    @Test
    void testVectorStorage() throws IOException {
        // Create a vector with mixed data types
        AVector vector = Vectors.of(
                Strings.create("test"),
                CVMLong.create(42L),
                Keyword.create("keyword"),
                Maps.of(Keyword.create("key"), Strings.create("value"))
        );

        Ref<AVector> ref = store.storeTopRef(Ref.get(vector), Ref.STORED, null);

        assertNotNull(ref);
        assertEquals(4, ref.getValue().count());

        // Retrieve and verify
        Hash hash = ref.getHash();
        Ref<AVector> retrieved = store.refForHash(hash);

        assertNotNull(retrieved);
        assertEquals(vector, retrieved.getValue());
        assertEquals("test", retrieved.getValue().get(0L).toString());
        assertEquals(42L, ((CVMLong) retrieved.getValue().get(1L)).longValue());
    }

    @Test
    void testRootHashManagement() throws IOException {
        // Initially should be NULL hash
        Hash initialRoot = store.getRootHash();
        assertEquals(null, initialRoot);

        // Set some root data
        String rootData = "This is root data";
        Ref<convex.core.data.AString> rootRef = store.setRootData(Strings.create(rootData));

        assertNotNull(rootRef);

        // Verify root hash changed
        Hash newRoot = store.getRootHash();
        assertNotEquals(Hash.NULL_HASH, newRoot);
        assertEquals(rootRef.getHash(), newRoot);

        // Verify we can retrieve root data
        convex.core.data.AString retrievedRoot = store.getRootData();
        assertEquals(rootData, retrievedRoot.toString());
    }

    @Test
    void testCaching() throws IOException {
        String testData = "Cache test data";
        Ref<convex.core.data.AString> ref1 = store.storeTopRef(Ref.get(Strings.create(testData)), Ref.STORED, null);

        // Second retrieval should come from cache
        Hash hash = ref1.getHash();
        Ref<convex.core.data.AString> ref2 = store.refForHash(hash);

        assertNotNull(ref2);
        assertEquals(ref1.getValue(), ref2.getValue());

        // Check cache directly
        Ref<convex.core.data.AString> cached = store.checkCache(hash);
        assertNotNull(cached);
        assertEquals(testData, cached.getValue().toString());
    }

    @Test
    void testNoveltyHandler() throws IOException {
		AStore oldStore = Stores.current();
		ArrayList<Ref<ACell>> al = new ArrayList<>();
		try {
			Stores.setCurrent(store);
			// create a random item that shouldn't already be in the store
			AVector<Blob> data = Vectors.of(Blob.createRandom(new Random(), 100),Blob.createRandom(new Random(), 100));

			// handler that records added refs
			Consumer<Ref<ACell>> handler = r -> al.add(r);

			Ref<AVector<Blob>> dataRef = data.getRef();
			Hash dataHash = dataRef.getHash();
			assertNull(store.refForHash(dataHash));

			Cells.announce(data,handler);
			int num=al.size(); // number of novel cells persisted
			assertTrue(num>0); // got new novelty
			assertEquals(data, al.get(num-1).getValue());

			data.getRef().persist();
			assertEquals(num, al.size()); // no new novelty transmitted
		} finally {
			Stores.setCurrent(oldStore);
		}
    }

    @Test
    void testEmbeddedValues() throws IOException {
        // Test that embedded values are handled correctly
        CVMLong smallNumber = CVMLong.create(42L);
        assertTrue(smallNumber.isEmbedded(), "Small numbers should be embedded");

        Ref<CVMLong> ref = store.storeTopRef(Ref.get(smallNumber), Ref.STORED, null);

        assertNotNull(ref);
        assertEquals(42L, ref.getValue().longValue());
    }

    @Test
    void testLargeDataStorage() throws IOException {
        // Create a large vector to test non-embedded storage
        Object[] largeArray = new Object[1000];
        for (int i = 0; i < 1000; i++) {
            largeArray[i] = Strings.create("Item " + i);
        }
        AVector largeVector = Vectors.of(largeArray);

        assertFalse(largeVector.isEmbedded(), "Large vectors should not be embedded");

        Ref<AVector> ref = store.storeTopRef(Ref.get(largeVector), Ref.STORED, null);

        assertNotNull(ref);
        assertEquals(1000, ref.getValue().count());

        // Retrieve and verify a few items
        Hash hash = ref.getHash();
        Ref<AVector> retrieved = store.refForHash(hash);

        assertNotNull(retrieved);
        assertEquals("Item 0", retrieved.getValue().get(0L).toString());
        assertEquals("Item 999", retrieved.getValue().get(999L).toString());
    }

    @Test
    void testStoreIntegrationWithGlobalStore() throws IOException {
        // Set PostgresStore as global store
        Stores.setCurrent(store);

        assertEquals(store, Stores.current());

        // Test that global store is used for operations
        String testData = "Global store test";
        Ref<convex.core.data.AString> ref = Ref.get(Strings.create(testData));

        // This should use the global store
        ref = ref.persist();

        assertNotNull(ref);
        assertEquals(testData, ref.getValue().toString());
        assertTrue(ref.getStatus() >= Ref.STORED);
    }

    @Test
    void testConnectionPooling() {
        // Test that the connection pool is working
        String poolStats = store.getPoolStats();
        assertNotNull(poolStats);
        assertTrue(poolStats.contains("Pool["));

        // The pool should have some connections available
        assertTrue(poolStats.contains("total="));
    }

    @Test
    void testStoreShortName() {
        String shortName = store.shortName();
        assertNotNull(shortName);
        assertTrue(shortName.startsWith("Postgres:"));
        assertTrue(shortName.contains("jdbc:postgresql://"));
    }

    @Test
    void testFromEnvironmentCreation() {
        // Test environment variable configuration
        // Note: This test doesn't actually set environment variables,
        // but tests the method exists and handles defaults
        assertDoesNotThrow(() -> {
            // This will use defaults since no env vars are set
            PostgresStore envStore = PostgresStore.fromEnvironment();
            assertNotNull(envStore);
            envStore.close();
        });
    }

    @Test
    void testNullHashHandling() throws IOException {
        // Test that null hash is handled correctly
        Ref<convex.core.data.AString> nullRef = store.refForHash(Hash.NULL_HASH);
        assertEquals(Ref.NULL_VALUE, nullRef);
    }

    @Test
    void testStoreStatusLevels() throws IOException {
        String testData = "Status level test";
        convex.core.data.AString cell = Strings.create(testData);

        // Test different status levels
        Ref<convex.core.data.AString> ref1 = store.storeTopRef(Ref.get(cell), Ref.STORED, null);
        assertEquals(Ref.STORED, ref1.getStatus());

        Ref<convex.core.data.AString> ref2 = store.storeTopRef(Ref.get(cell), Ref.PERSISTED, null);
        assertTrue(ref2.getStatus() >= Ref.PERSISTED);
    }

    @Test
    void testStoreTopRef() throws IOException {
        // Test equivalent to EtchStoreTest.testStoreTopRef
        AVector<CVMLong> testVector = Vectors.of(CVMLong.create(0), CVMLong.create(66758585));
        Ref<AVector<CVMLong>> originalRef = testVector.getRef();
        Hash hash = testVector.getHash();

        // Store the top ref
        Ref<AVector<CVMLong>> storedRef = store.storeTopRef(originalRef, Ref.STORED, null);
        assertEquals(hash, storedRef.getHash());

        // Verify it can be retrieved
        Ref<AVector<CVMLong>> retrievedRef = store.readStoreRef(hash);
        assertNotNull(retrievedRef);
        assertEquals(testVector, retrievedRef.getValue());
    }

    @Test
    void testPersistInternal() {
		// an example internal definition
		ACell c=Keywords.ADDRESS;
		
		// Interning is idempotent
		assertSame(c,Cells.intern(c));
    }

    @Test
    void testPersistedStatus() throws IOException {
        // Test persistence status tracking
        // generate Hash of unique secure random bytes to test - should not already be
        // in store
        Stores.setCurrent(store);

        Blob randomBlob = Blob.createRandom(new Random(), Format.MAX_EMBEDDED_LENGTH+1);
        Hash hash = randomBlob.getHash();
        assertNotEquals(hash, randomBlob);

        Ref<Blob> initialRef = randomBlob.getRef();
        assertEquals(Ref.UNKNOWN, initialRef.getStatus());
        assertNull(Stores.current().refForHash(hash));

        // shallow persistence first
        Ref<Blob> refShallow=initialRef.persistShallow();
        assertEquals(Ref.STORED, refShallow.getStatus());

        Ref<Blob> ref = initialRef.persist();
        assertEquals(Ref.PERSISTED, ref.getStatus());
        assertTrue(ref.isPersisted());

        Ref<Blob> newRef = Stores.current().refForHash(hash);
        assertEquals(initialRef, newRef);
        assertEquals(randomBlob, newRef.getValue());
    }

    @Test
    void testStoreReopen() throws IOException, SQLException {
        // Test store reopening functionality
        String testData = "reopen test data";
        convex.core.data.AString cell = Strings.create(testData);

        Ref<ACell> r=store.storeTopRef(cell.getRef(), Ref.STORED, null);
        Hash hash = cell.getHash();

    
        // Close current store
        store.close();

        // Create new store instance with same connection
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        PostgresStore newStore = new PostgresStore(jdbcUrl, username, password);

        // Verify data is still accessible
        Ref<convex.core.data.AString> retrievedRef = newStore.refForHash(hash);
        assertNotNull(retrievedRef);
        assertEquals(testData, retrievedRef.getValue().toString());

        newStore.close();
    }

    @Test
    void testBeliefAnnounce() throws IOException {
        // Test belief announcement functionality equivalent to EtchStoreTest.testBeliefAnnounce
        AStore oldStore = Stores.current();
        AtomicLong counter=new AtomicLong(0L);
		AKeyPair kp=InitTest.HERO_KEYPAIR;

        try {
            Stores.setCurrent(store);

			ATransaction t1=Invoke.create(InitTest.HERO,0, Lists.of(Symbols.PLUS, Symbols.STAR_BALANCE, 1000L));
			ATransaction t2=Transfer.create(InitTest.HERO,1, InitTest.VILLAIN,1000000);
			Block b=Block.of(Utils.getCurrentTimestamp(),kp.signData(t1),kp.signData(t2));

			Order ord=Order.create().append(kp.signData(b));

			Belief belief=Belief.create(kp,ord);

			Ref<Belief> rb=belief.getRef();
			Ref<ATransaction> rt=t1.getRef();
			assertEquals(Ref.UNKNOWN,rb.getStatus());
			assertEquals(Ref.UNKNOWN,rt.getStatus());

			assertEquals(3,Cells.refCount(t1));
			assertEquals(0,Cells.refCount(t2));
			assertEquals(14,Refs.totalRefCount(belief));


			Consumer<Ref<ACell>> noveltyHandler=r-> {
				counter.incrementAndGet();
			};

			// First try shallow persistence
			counter.set(0L);
			Ref<Belief> srb=rb.persistShallow(noveltyHandler);
			assertEquals(Ref.STORED,srb.getStatus());
			// One cell persisted, should only be novelty if embedded
			assertEquals(belief.isEmbedded()?0L:1L,counter.get()); 

			// assertEquals(srb,store.refForHash(rb.getHash()));
			assertNull(store.refForHash(t1.getRef().getHash()));

			// Persist belief
			counter.set(0L);
			Ref<Belief> prb=srb.persist(noveltyHandler);
			assertEquals(4L,counter.get());

			// Persist again. Should be no new novelty
			counter.set(0L);
			Ref<Belief> prb2=srb.persist(noveltyHandler);
			assertEquals(prb2,prb);
			assertEquals(0L,counter.get()); // Nothing new persisted

			// Announce belief
			counter.set(0L);
			Ref<Belief> arb=Cells.announce(belief,noveltyHandler).getRef();
			assertEquals(srb,arb);
			assertEquals(4L,counter.get());

			// Announce again. Should be no new novelty
			counter.set(0L);
			Ref<Belief> arb2=Cells.announce(belief,noveltyHandler).getRef();
			assertEquals(srb,arb2);
			assertEquals(0L,counter.get()); // Nothing new announced

			// Check re-stored ref has correct status
			counter.set(0L);
			Ref<Belief> arb3=srb.persistShallow(noveltyHandler);
			assertEquals(0L,counter.get()); // Nothing new persisted
			assertTrue(Ref.STORED<=arb3.getStatus());

			// Recover Belief from store. Should be top level stored
			Belief recb=(Belief) store.refForHash(belief.getHash()).getValue();
			assertEquals(belief,recb);

        } finally {
            Stores.setCurrent(oldStore);
        }
    }
}