package convex.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import convex.core.data.AVector;
import convex.core.data.Hash;
import convex.core.data.Keyword;
import convex.core.data.Maps;
import convex.core.data.Ref;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.core.store.Stores;
import convex.core.store.MemoryStore;
// EtchStore removed - PostgreSQL only

/**
 * Comprehensive tests for PostgresStore using TestContainers
 */
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
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
        // Reset global store to default to prevent test interference
        try {
            Stores.setGlobalStore(new MemoryStore());
        } catch (Exception e) {
            // Ignore reset errors, other tests will create their own stores
        }
    }

    private void createSchema() throws SQLException {
        try (Connection conn = postgres.createConnection("");
             Statement stmt = conn.createStatement()) {

            // Create schema
            stmt.execute("CREATE SCHEMA IF NOT EXISTS convex");

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
        assertEquals(Hash.NULL_HASH, initialRoot);

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
        final boolean[] noveltyDetected = {false};

        String testData = "Novelty test";
        store.storeTopRef(
                Ref.get(Strings.create(testData)),
                Ref.STORED,
                ref -> noveltyDetected[0] = true
        );

        assertTrue(noveltyDetected[0], "Novelty handler should be called for new data");

        // Reset and store the same data again
        noveltyDetected[0] = false;
        store.storeTopRef(
                Ref.get(Strings.create(testData)),
                Ref.STORED,
                ref -> noveltyDetected[0] = true
        );

        // Should not trigger novelty handler for existing data
        assertFalse(noveltyDetected[0], "Novelty handler should not be called for existing data");
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
        Stores.setGlobalStore(store);

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
    void testPersistInternal() throws IOException {
        // Test internal persistence mechanisms
        AVector<convex.core.data.AString> testData = Vectors.of(
            Strings.create("test1"),
            Strings.create("test2"),
            Strings.create("test3")
        );

        Ref<AVector<convex.core.data.AString>> ref = testData.getRef();

        // Test persistence at different levels
        ref.persist();
        assertTrue(ref.getStatus() >= Ref.PERSISTED);

        // Verify all child refs are also persisted
        for (int i = 0; i < testData.size(); i++) {
            Ref<convex.core.data.AString> childRef = testData.get(i).getRef();
            assertTrue(childRef.getStatus() >= Ref.PERSISTED);
        }
    }

    @Test
    void testPersistedStatus() throws IOException {
        // Test persistence status tracking
        convex.core.data.AString testCell = Strings.create("persistence test");
        Ref<convex.core.data.AString> ref = testCell.getRef();

        // Initially not persisted
        assertTrue(ref.getStatus() < Ref.PERSISTED);

        // After persistence
        ref.persist();
        assertTrue(ref.getStatus() >= Ref.PERSISTED);

        // Verify status is maintained after retrieval
        Hash hash = testCell.getHash();
        Ref<convex.core.data.AString> retrievedRef = store.refForHash(hash);
        assertNotNull(retrievedRef);
        assertTrue(retrievedRef.getStatus() >= Ref.PERSISTED);
    }

    @Test
    void testStoreReopen() throws IOException, SQLException {
        // Test store reopening functionality
        String testData = "reopen test data";
        convex.core.data.AString cell = Strings.create(testData);
        Hash hash = cell.getHash();

        // Store data in current store
        cell.getRef().persist();

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
        try {
            Stores.setCurrent(store);

            // Create test data similar to the Etch test
            convex.core.data.AString data1 = Strings.create("belief test 1");
            convex.core.data.AString data2 = Strings.create("belief test 2");
            AVector<convex.core.data.AString> testVector = Vectors.of(data1, data2);

            Ref<AVector<convex.core.data.AString>> vectorRef = testVector.getRef();
            Hash vectorHash = vectorRef.getHash();

            // Initially should not be in store
            assertEquals(Ref.UNKNOWN, vectorRef.getStatus());
            assertNull(store.refForHash(vectorHash));

            // Announce the data (similar to belief announcement)
            vectorRef.persist();

            // Verify it's now in the store
            assertTrue(vectorRef.getStatus() >= Ref.PERSISTED);
            Ref<AVector<convex.core.data.AString>> retrievedRef = store.refForHash(vectorHash);
            assertNotNull(retrievedRef);
            assertEquals(testVector, retrievedRef.getValue());

        } finally {
            Stores.setCurrent(oldStore);
        }
    }
}