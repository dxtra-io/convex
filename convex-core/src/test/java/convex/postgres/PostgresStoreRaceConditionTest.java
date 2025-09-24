package convex.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import convex.core.data.AVector;
import convex.core.data.AccountKey;
import convex.core.data.Blob;
import convex.core.data.Cells;
import convex.core.data.Hash;
import convex.core.data.Keyword;
import convex.core.data.Keywords;
import convex.core.data.Lists;
import convex.core.data.Maps;
import convex.core.data.Ref;
import convex.core.data.SignedData;
import convex.core.data.Strings;
import convex.core.data.Vectors;
import convex.core.data.prim.CVMLong;
import convex.core.exceptions.MissingDataException;
import convex.core.init.InitTest;
import convex.core.lang.Symbols;
import convex.core.store.MemoryStore;
import convex.core.store.Stores;
import convex.core.transactions.ATransaction;
import convex.core.transactions.Invoke;
import convex.core.transactions.Transfer;
import convex.core.util.Utils;

/**
 * Comprehensive race condition tests for PostgresStore that simulate the exact
 * conditions that were causing MissingDataException during contract deployment.
 *
 * These tests verify that the atomic transaction fixes eliminate race conditions
 * between concurrent cell storage and retrieval operations.
 */
@Testcontainers
public class PostgresStoreRaceConditionTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15.8-alpine")
            .withDatabaseName("convex_test")
            .withUsername("test_user")
            .withPassword("test_pass");

    private PostgresStore store;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() throws SQLException {
        createSchema();

        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        store = new PostgresStore(jdbcUrl, username, password);
        executorService = Executors.newFixedThreadPool(10);

        // Set as global store for testing
        Stores.setGlobalStore(store);
        Stores.setCurrent(store);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        }

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

            stmt.execute("CREATE SCHEMA IF NOT EXISTS convex");

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS convex.cells (
                    hash VARCHAR(64) PRIMARY KEY,
                    encoding BYTEA NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0,
                    size INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS convex.root (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    root_hash VARCHAR(64),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT single_root CHECK (id = 1)
                )
            """);

            stmt.execute("INSERT INTO convex.root (id, root_hash) VALUES (1, NULL) ON CONFLICT (id) DO NOTHING");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cells_hash ON convex.cells USING HASH(hash)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cells_status ON convex.cells(status)");
        }
    }

    /**
     * Test that simulates the exact conditions that caused MissingDataException:
     * - Multiple threads storing cells simultaneously (BeliefPropagator scenario)
     * - Other threads attempting to read those cells (account registration scenario)
     * - High concurrency to stress the atomic locking mechanisms
     */
    @Test
    void testConcurrentStorageAndRetrievalNoMissingData() throws Exception {
        final int NUM_STORAGE_THREADS = 5;
        final int NUM_READ_THREADS = 5;
        final int CELLS_PER_THREAD = 20;

        // Create test data similar to blockchain cells
        List<ACell> testCells = createBlockchainTestData(NUM_STORAGE_THREADS * CELLS_PER_THREAD);

        AtomicInteger missingDataExceptions = new AtomicInteger(0);
        AtomicInteger successfulStores = new AtomicInteger(0);
        AtomicInteger successfulReads = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Storage threads (simulate BeliefPropagator)
        for (int i = 0; i < NUM_STORAGE_THREADS; i++) {
            final int threadIndex = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int j = 0; j < CELLS_PER_THREAD; j++) {
                        ACell cell = testCells.get(threadIndex * CELLS_PER_THREAD + j);
                        Ref<ACell> ref = store.storeTopRef(Ref.get(cell), Ref.PERSISTED, null);

                        assertNotNull(ref, "Store operation should not return null");
                        assertTrue(ref.getStatus() >= Ref.STORED, "Cell should be marked as stored");

                        successfulStores.incrementAndGet();

                        // Small delay to increase chance of race conditions
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    if (e instanceof MissingDataException ||
                        (e.getCause() instanceof MissingDataException)) {
                        missingDataExceptions.incrementAndGet();
                    }
                    fail("Storage thread failed: " + e.getMessage(), e);
                }
            }, executorService));
        }

        // Add small delay before starting readers
        Thread.sleep(10);

        // Reader threads (simulate account registration/contract deployment)
        for (int i = 0; i < NUM_READ_THREADS; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    // Continuously attempt to read cells as they're being stored
                    for (int attempt = 0; attempt < 100; attempt++) {
                        for (ACell cell : testCells) {
                            try {
                                Hash hash = Hash.get(cell);
                                Ref<ACell> retrievedRef = store.refForHash(hash);

                                if (retrievedRef != null) {
                                    assertEquals(cell, retrievedRef.getValue(),
                                               "Retrieved cell should match original");
                                    successfulReads.incrementAndGet();
                                }

                            } catch (Exception e) {
                                if (e instanceof MissingDataException ||
                                    (e.getCause() instanceof MissingDataException)) {
                                    missingDataExceptions.incrementAndGet();
                                    fail("MissingDataException should not occur with race condition fixes: " + e.getMessage());
                                }
                                // Other exceptions are acceptable (cell not yet stored)
                            }
                        }

                        Thread.sleep(5);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, executorService));
        }

        // Wait for all operations to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(60, TimeUnit.SECONDS);

        // Verify results
        assertEquals(0, missingDataExceptions.get(),
                    "No MissingDataException should occur with atomic storage fixes");

        assertTrue(successfulStores.get() > 0,
                  "At least some storage operations should succeed");

        assertTrue(successfulReads.get() > 0,
                  "At least some read operations should succeed");

        // Verify all test data is eventually stored and retrievable
        for (ACell cell : testCells) {
            Hash hash = Hash.get(cell);
            Ref<ACell> ref = store.refForHash(hash);
            assertNotNull(ref, "All cells should be retrievable after operations complete");
            assertEquals(cell, ref.getValue(), "Retrieved cells should match originals");
        }

        System.out.println("✅ Race condition test PASSED:");
        System.out.println("   - Successful stores: " + successfulStores.get());
        System.out.println("   - Successful reads: " + successfulReads.get());
        System.out.println("   - MissingDataException count: " + missingDataExceptions.get());
    }

    /**
     * Test the exact sequence that was failing:
     * 1. Account registration starts (reads blockchain state)
     * 2. Contract deployment begins (stores new cells)
     * 3. Account registration continues (expects cells to be available)
     */
    @Test
    void testAccountRegistrationContractDeploymentSequence() throws Exception {
        // Create data similar to what account registration would encounter
        AKeyPair keyPair = InitTest.HERO_KEYPAIR;
        AccountKey peerKey = AccountKey.create(keyPair.getAccountKey());

        // Create transaction similar to account registration
        ATransaction registerTx = Invoke.create(InitTest.HERO, 0,
                                               Lists.of(Symbols.PLUS, Keywords.NAME, "test-account"));

        // Create block containing the transaction
        Block block = Block.of(Utils.getCurrentTimestamp(), keyPair.signData(registerTx));
        Order order = Order.create().append(keyPair.signData(block));
        Belief belief = Belief.create(keyPair, order);

        AtomicLong noveltyCount = new AtomicLong(0);
        AtomicInteger missingDataErrors = new AtomicInteger(0);
        Consumer<Ref<ACell>> noveltyHandler = r -> noveltyCount.incrementAndGet();

        // Simulate concurrent operations
        CompletableFuture<Void> storeOperation = CompletableFuture.runAsync(() -> {
            try {
                // This simulates BeliefPropagator storing blockchain state
                Ref<Belief> storedBelief = Cells.announce(belief, noveltyHandler).getRef();
                assertNotNull(storedBelief, "Belief should be stored successfully");
                assertTrue(storedBelief.getStatus() >= Ref.STORED, "Belief should be marked as stored");

            } catch (Exception e) {
                if (e instanceof MissingDataException) {
                    missingDataErrors.incrementAndGet();
                }
                fail("Belief storage should not fail: " + e.getMessage(), e);
            }
        }, executorService);

        CompletableFuture<Void> readOperation = CompletableFuture.runAsync(() -> {
            try {
                // Small delay to ensure storage starts first
                Thread.sleep(5);

                // This simulates account registration reading blockchain state
                Hash beliefHash = belief.getHash();

                // Keep trying to read until it's available (with timeout)
                Ref<Belief> retrievedBelief = null;
                int attempts = 0;
                while (retrievedBelief == null && attempts < 100) {
                    try {
                        retrievedBelief = store.refForHash(beliefHash);
                        if (retrievedBelief == null) {
                            Thread.sleep(10);
                            attempts++;
                        }
                    } catch (MissingDataException e) {
                        missingDataErrors.incrementAndGet();
                        fail("MissingDataException during belief retrieval: " + e.getMessage());
                    }
                }

                assertNotNull(retrievedBelief, "Should eventually be able to read stored belief");
                assertEquals(belief, retrievedBelief.getValue(), "Retrieved belief should match original");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, executorService);

        // Wait for both operations to complete
        CompletableFuture.allOf(storeOperation, readOperation).get(30, TimeUnit.SECONDS);

        // Verify results
        assertEquals(0, missingDataErrors.get(), "No MissingDataException should occur");
        assertTrue(noveltyCount.get() > 0, "Novelty handler should be called for new data");

        System.out.println("✅ Account registration + contract deployment test PASSED");
    }

    /**
     * High-stress test with many threads performing mixed read/write operations
     * to verify atomic locking works under extreme conditions.
     */
    @Test
    void testHighConcurrencyMixedOperations() throws Exception {
        final int NUM_THREADS = 20;
        final int OPERATIONS_PER_THREAD = 50;

        List<ACell> sharedCells = createBlockchainTestData(100);
        AtomicInteger exceptions = new AtomicInteger(0);
        AtomicInteger operations = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                Random random = new Random();

                for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                    try {
                        ACell cell = sharedCells.get(random.nextInt(sharedCells.size()));

                        if (random.nextBoolean()) {
                            // Store operation
                            store.storeTopRef(Ref.get(cell), Ref.PERSISTED, null);
                        } else {
                            // Read operation
                            Hash hash = Hash.get(cell);
                            store.refForHash(hash);
                        }

                        operations.incrementAndGet();

                    } catch (Exception e) {
                        if (e instanceof MissingDataException ||
                            (e.getCause() instanceof MissingDataException)) {
                            exceptions.incrementAndGet();
                        }
                        // Other exceptions are acceptable in high-concurrency scenarios
                    }
                }
            }, executorService));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(60, TimeUnit.SECONDS);

        assertEquals(0, exceptions.get(),
                    "No MissingDataException should occur even under high concurrency");

        assertTrue(operations.get() > NUM_THREADS * OPERATIONS_PER_THREAD / 2,
                  "Most operations should complete successfully");

        System.out.println("✅ High concurrency test PASSED:");
        System.out.println("   - Total operations: " + operations.get());
        System.out.println("   - MissingDataException count: " + exceptions.get());
    }

    /**
     * Test atomic consistency: verify that partially stored hierarchical data
     * (like beliefs with child transactions) cannot be read in incomplete state.
     */
    @Test
    void testAtomicHierarchicalStorage() throws Exception {
        AKeyPair keyPair = InitTest.HERO_KEYPAIR;
        AccountKey peerKey = AccountKey.create(keyPair.getAccountKey());

        // Create complex hierarchical data
        ATransaction tx1 = Invoke.create(InitTest.HERO, 0, Lists.of(Symbols.PLUS, CVMLong.create(1)));
        ATransaction tx2 = Transfer.create(InitTest.HERO, 1, InitTest.VILLAIN, 1000);
        Block block = Block.of(Utils.getCurrentTimestamp(), keyPair.signData(tx1), keyPair.signData(tx2));
        Order order = Order.create().append(keyPair.signData(block));
        Belief belief = Belief.create(keyPair, order);

        AtomicInteger readersSeenIncompleteData = new AtomicInteger(0);
        AtomicInteger missingDataErrors = new AtomicInteger(0);

        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(() -> {
            try {
                // Store the belief (which should atomically store all child refs)
                Ref<Belief> storedRef = store.storeTopRef(Ref.get(belief), Ref.PERSISTED, null);
                assertNotNull(storedRef, "Belief storage should succeed");

            } catch (Exception e) {
                if (e instanceof MissingDataException) {
                    missingDataErrors.incrementAndGet();
                }
                fail("Belief storage failed: " + e.getMessage(), e);
            }
        }, executorService);

        // Multiple readers trying to access the data as it's being stored
        List<CompletableFuture<Void>> readerFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            readerFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    for (int attempt = 0; attempt < 50; attempt++) {
                        try {
                            Hash beliefHash = belief.getHash();
                            Ref<Belief> retrieved = store.refForHash(beliefHash);

                            if (retrieved != null) {
                                // If we can read the belief, we should be able to read all its children
                                Belief retrievedBelief = retrieved.getValue();
                                Order retrievedOrder = retrievedBelief.getOrder(peerKey);

                                assertNotNull(retrievedOrder, "Order should be accessible if belief is readable");

                                // Try to access child transactions
                                for (int blockIndex = 0; blockIndex < retrievedOrder.getBlockCount(); blockIndex++) {
                                    SignedData<Block> signedBlock = retrievedOrder.getBlock(blockIndex);
                                    assertNotNull(signedBlock, "Signed block should be accessible");

                                    Block retrievedBlock = signedBlock.getValue();
                                    assertNotNull(retrievedBlock, "Block should be accessible");

                                    AVector<SignedData<ATransaction>> transactions = retrievedBlock.getTransactions();
                                    for (int txIndex = 0; txIndex < transactions.count(); txIndex++) {
                                        SignedData<ATransaction> signedTx = transactions.get(txIndex);
                                        assertNotNull(signedTx, "Signed transaction should be accessible");

                                        ATransaction retrievedTx = signedTx.getValue();
                                        assertNotNull(retrievedTx, "Transaction should be accessible");
                                    }
                                }
                            }

                        } catch (MissingDataException e) {
                            readersSeenIncompleteData.incrementAndGet();
                            missingDataErrors.incrementAndGet();
                            fail("Readers should never see incomplete hierarchical data: " + e.getMessage());
                        }

                        Thread.sleep(2);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, executorService));
        }

        // Wait for all operations
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();
        allFutures.add(writerFuture);
        allFutures.addAll(readerFutures);

        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0]))
                        .get(30, TimeUnit.SECONDS);

        assertEquals(0, readersSeenIncompleteData.get(),
                    "Readers should never encounter incomplete hierarchical data");
        assertEquals(0, missingDataErrors.get(),
                    "No MissingDataException should occur");

        System.out.println("✅ Atomic hierarchical storage test PASSED");
    }

    /**
     * Create test data that resembles blockchain structures to closely simulate
     * the conditions that were causing MissingDataException.
     */
    private List<ACell> createBlockchainTestData(int count) {
        List<ACell> cells = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            switch (i % 4) {
                case 0:
                    // String data (like DIDs)
                    cells.add(Strings.create("test-data-" + i));
                    break;

                case 1:
                    // Binary data (like hashes)
                    cells.add(Blob.createRandom(random, 32));
                    break;

                case 2:
                    // Structured data (like transactions)
                    cells.add(Maps.of(
                        Keyword.intern("type"), Strings.create("transaction"),
                        Keyword.intern("id"), CVMLong.create(i),
                        Keyword.intern("data"), Strings.create("payload-" + i)
                    ));
                    break;

                case 3:
                    // Collections (like blocks)
                    cells.add(Vectors.of(
                        CVMLong.create(i),
                        Strings.create("block-data-" + i),
                        Blob.createRandom(random, 16)
                    ));
                    break;
            }
        }

        return cells;
    }
}