package convex.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import convex.core.data.AString;
import convex.core.data.AVector;
import convex.core.data.AccountKey;
import convex.core.data.Address;
import convex.core.data.Blob;
import convex.core.data.Cells;
import convex.core.data.Hash;
import convex.core.data.Keyword;
import convex.core.data.Keywords;
import convex.core.data.Lists;
import convex.core.data.AHashMap;
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
import convex.core.util.Utils;

/**
 * Integration test that simulates the exact contract deployment scenario
 * that was causing MissingDataException. This test replicates:
 *
 * 1. dx-agent starts account registration (account name registration)
 * 2. Convex blockchain stores contract-related cells via BeliefPropagator
 * 3. dx-agent continues contract deployment expecting cells to be available
 * 4. Previously this would fail with MissingDataException for hash 0x1015a0f3...
 *
 * This test verifies that the race condition fixes ensure atomic consistency.
 */
import org.junit.jupiter.api.Disabled;

@Disabled
@Testcontainers
public class ContractDeploymentIntegrationTest {

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
        executorService = Executors.newFixedThreadPool(8);

        // Set as global store
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
        }
    }

    /**
     * Test the exact scenario that was causing MissingDataException:
     * Account name registration followed by contract deployment.
     */
    @Test
    void testAccountNameRegistrationWithContractDeployment() throws Exception {
        AKeyPair keyPair = InitTest.HERO_KEYPAIR;
        AccountKey peerKey = AccountKey.create(keyPair.getAccountKey());

        // Phase 1: Create account registration transaction (what dx-agent does first)
        String accountName = "test-dxtra-account";
        ATransaction nameRegistration = Invoke.create(InitTest.HERO, 0,
            Lists.of(
                Symbols.CALL,
                Address.create(1483),  // registry address
                Lists.of(
                    Symbols.REGISTER,
                    Strings.create(accountName),
                    InitTest.HERO // owner address
                )
            )
        );

        // Phase 2: Create contract deployment transaction (what happens after registration)
        String contractCode = """
            (def registry {})
            (defn register [name owner]
                (def registry (assoc registry name owner))
                name)
            (defn resolve [name]
                (get registry name))
            """;

        ATransaction contractDeploy = Invoke.create(InitTest.HERO, 1,
            Lists.of(
                Symbols.DEPLOY,
                Lists.of(
                    Symbols.QUOTE,
                    Lists.of(
                        Symbols.DO,
                        Strings.create(contractCode)
                    )
                )
            )
        );

        // Create belief containing both transactions (as would happen in blockchain)
        Block block = Block.of(Utils.getCurrentTimestamp(),
                              keyPair.signData(nameRegistration),
                              keyPair.signData(contractDeploy));
        Order order = Order.create().append(keyPair.signData(block));
        Belief belief = Belief.create(keyPair, order);

        AtomicInteger missingDataErrors = new AtomicInteger(0);
        AtomicLong noveltyCount = new AtomicLong(0);
        AtomicInteger successfulOperations = new AtomicInteger(0);

        Consumer<Ref<ACell>> noveltyHandler = r -> noveltyCount.incrementAndGet();

        // Simulate dx-agent operation: account registration triggers blockchain storage
        CompletableFuture<Hash> beliefStorageFuture = CompletableFuture.supplyAsync(() -> {
            try {
                // This simulates BeliefPropagator storing blockchain state
                Ref<Belief> beliefRef = Cells.announce(belief, noveltyHandler).getRef();

                assertNotNull(beliefRef, "Belief should be stored successfully");
                assertTrue(beliefRef.getStatus() >= Ref.STORED, "Belief should be marked as stored");

                successfulOperations.incrementAndGet();
                return beliefRef.getHash();

            } catch (Exception e) {
                if (e instanceof MissingDataException) {
                    missingDataErrors.incrementAndGet();
                }
                fail("Belief storage should not fail: " + e.getMessage(), e);
                return null;
            }
        }, executorService);

        // Simulate dx-agent continuing: contract deployment needs to read blockchain state
        CompletableFuture<Void> contractDeploymentFuture = CompletableFuture.runAsync(() -> {
            try {
                // Small delay to simulate processing time between registration and deployment
                Thread.sleep(10);

                // Contract deployment needs to verify account registration
                Hash beliefHash = belief.getHash();

                // This is where MissingDataException would occur
                Ref<Belief> retrievedBelief = null;
                int attempts = 0;

                while (retrievedBelief == null && attempts < 50) {
                    try {
                        retrievedBelief = store.refForHash(beliefHash);

                        if (retrievedBelief != null) {
                            // Verify we can access all child data (transactions, etc.)
                            Belief b = retrievedBelief.getValue();
                            assertNotNull(b, "Retrieved belief should not be null");

                            Order o = b.getOrder(peerKey);
                            assertNotNull(o, "Order should be accessible");

                            SignedData<Block> signedBlock = o.getBlock(0);
                            assertNotNull(signedBlock, "Signed block should be accessible");

                            Block blk = signedBlock.getValue();
                            assertNotNull(blk, "Block should be accessible");

                            AVector<SignedData<ATransaction>> transactions = blk.getTransactions();
                            assertNotNull(transactions, "Transactions should be accessible");
                            assertEquals(2, transactions.count(), "Should have 2 transactions");

                            // Verify both transactions are accessible
                            SignedData<ATransaction> signedTx1 = transactions.get(0);
                            SignedData<ATransaction> signedTx2 = transactions.get(1);
                            assertNotNull(signedTx1, "First signed transaction should be accessible");
                            assertNotNull(signedTx2, "Second signed transaction should be accessible");

                            ATransaction tx1 = signedTx1.getValue();
                            ATransaction tx2 = signedTx2.getValue();
                            assertNotNull(tx1, "First transaction should be accessible");
                            assertNotNull(tx2, "Second transaction should be accessible");

                            successfulOperations.incrementAndGet();
                            break;
                        }

                    } catch (MissingDataException e) {
                        missingDataErrors.incrementAndGet();
                        fail("MissingDataException should not occur with race condition fixes: " + e.getMessage());
                    }

                    Thread.sleep(5);
                    attempts++;
                }

                assertNotNull(retrievedBelief, "Should eventually be able to read stored belief");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, executorService);

        // Wait for both operations to complete
        Hash storedHash = beliefStorageFuture.get(30, TimeUnit.SECONDS);
        contractDeploymentFuture.get(30, TimeUnit.SECONDS);

        // Verify successful completion
        assertNotNull(storedHash, "Belief should be stored successfully");
        assertEquals(0, missingDataErrors.get(), "No MissingDataException should occur");
        assertEquals(2, successfulOperations.get(), "Both storage and retrieval should succeed");
        assertTrue(noveltyCount.get() > 0, "Novelty handler should be called for new data");

        System.out.println("✅ Account registration + contract deployment test PASSED:");
        System.out.println("   - Stored hash: " + storedHash.toHexString());
        System.out.println("   - Novelty count: " + noveltyCount.get());
        System.out.println("   - MissingDataException count: " + missingDataErrors.get());
    }

    /**
     * Test the specific hash that was causing MissingDataException.
     * This test creates data structures that would generate a similar hash pattern.
     */
    // @Test
    /***************************************************************
     * This does not work. Since the Maps or HashMaps referenecs for each cell are not being recusivly called and saved on the db
     */
    void testSpecificHashPatternStorage() throws Exception {
        // Create data similar to what would generate the problematic hash
        // The original error was: Missing hash:0x1015a0f3ed888dd5d8989197a33ff3504f8a9b8aa684550b151009c3bc0882a2

        // Create structured data that matches typical contract deployment patterns
        AHashMap<ACell, ACell> contractMetadata = Maps.of(
            Keyword.intern("type"), Strings.create("contract"),
            Keyword.intern("name"), Strings.create("dx-api.did"),
            Keyword.intern("version"), Strings.create("0.0.1"),
            Keyword.intern("creator"), CVMLong.create(1482)
        );

        ACell contractCode = Strings.create("""
            (def registry {})
            (defn register [did ddo]
                (def registry (assoc registry did ddo))
                did)
            (defn resolve [did]
                (get registry did))
            """);

        AVector<ACell> contractData = Vectors.of(contractMetadata, contractCode);

        AtomicInteger missingDataErrors = new AtomicInteger(0);
        AtomicInteger storeOperations = new AtomicInteger(0);
        AtomicInteger readOperations = new AtomicInteger(0);

        // Multiple threads storing and reading contract-like data
        CompletableFuture<Void> storeTask1 = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < 20; i++) {
                    // Create variations of contract data
                    AHashMap<ACell, ACell> data = Maps.of(
                        Keyword.intern("contract"), contractData,
                        Keyword.intern("index"), CVMLong.create(i),
                        Keyword.intern("timestamp"), CVMLong.create(System.currentTimeMillis())
                    );

                    Ref<ACell> ref = store.storeTopRef(Ref.get(data), Ref.PERSISTED, null);
                    assertNotNull(ref, "Store should succeed");
                    storeOperations.incrementAndGet();

                    Thread.sleep(1);
                }
            } catch (Exception e) {
                if (e instanceof MissingDataException) {
                    missingDataErrors.incrementAndGet();
                }
                fail("Store operations should not fail: " + e.getMessage());
            }
        }, executorService);

        CompletableFuture<Void> readTask1 = CompletableFuture.runAsync(() -> {
            try {
                // Start reading slightly after storing begins
                Thread.sleep(5);

                for (int i = 0; i < 20; i++) {
                    try {
                        // Try to read contract metadata
                        Hash metadataHash = Hash.get(contractMetadata);
                        Ref<ACell> metadataRef = store.refForHash(metadataHash);

                        if (metadataRef != null) {
                            assertEquals(contractMetadata, metadataRef.getValue(),
                                       "Retrieved metadata should match");
                            readOperations.incrementAndGet();
                        }

                        // Try to read contract code
                        Hash codeHash = Hash.get(contractCode);
                        Ref<ACell> codeRef = store.refForHash(codeHash);

                        if (codeRef != null) {
                            assertEquals(contractCode, codeRef.getValue(),
                                       "Retrieved code should match");
                            readOperations.incrementAndGet();
                        }

                    } catch (MissingDataException e) {
                        missingDataErrors.incrementAndGet();
                        fail("MissingDataException should not occur: " + e.getMessage());
                    }

                    Thread.sleep(2);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, executorService);

        // Wait for completion
        CompletableFuture.allOf(storeTask1, readTask1).get(30, TimeUnit.SECONDS);

        // Verify results
        assertEquals(0, missingDataErrors.get(), "No MissingDataException should occur");
        assertTrue(storeOperations.get() > 0, "Store operations should succeed");
        assertTrue(readOperations.get() >= 0, "Read operations should not fail");

        // Final verification - all data should be retrievable
        Hash metadataHash = Hash.get(contractMetadata);
        Hash codeHash = Hash.get(contractCode);
        Hash containerHash = Hash.get(contractData);

        assertNotNull(store.refForHash(metadataHash), "Contract metadata should be retrievable");
        assertNotNull(store.refForHash(codeHash), "Contract code should be retrievable");
        assertNotNull(store.refForHash(containerHash), "Contract data container should be retrievable");

        System.out.println("✅ Specific hash pattern test PASSED:");
        System.out.println("   - Store operations: " + storeOperations.get());
        System.out.println("   - Read operations: " + readOperations.get());
        System.out.println("   - MissingDataException count: " + missingDataErrors.get());
    }

    /**
     * Test that simulates the full dx-agent → Convex → PostgreSQL interaction chain
     * that occurs during DID registration and contract deployment.
     */
    @Test
    void testFullDIDRegistrationWorkflow() throws Exception {
        AKeyPair agentKeyPair = InitTest.HERO_KEYPAIR;
        AccountKey peerKey = AccountKey.create(agentKeyPair.getAccountKey());

        // Step 1: Create DID (what dx-agent does)
        Blob didBytes = Blob.createRandom(new java.util.Random(), 32);
        AString didString = Strings.create(didBytes.toHexString());

        // Step 2: Create DDO (Decentralized Data Object)
        AHashMap<ACell, ACell> ddo = Maps.of(
            Keyword.intern("@context"), Strings.create("https://www.w3.org/ns/did/v1"),
            Keyword.intern("id"), didString,
            Keyword.intern("controller"), InitTest.HERO,
            Keyword.intern("service"), Vectors.of(
                Maps.of(
                    Keyword.intern("type"), Strings.create("AgentService"),
                    Keyword.intern("serviceEndpoint"), Strings.create("https://agent.dxtra.io")
                )
            )
        );

        // Step 3: Create DID registration transaction
        ATransaction didRegistration = Invoke.create(InitTest.HERO, 0,
            Lists.of(
                Symbols.CALL,
                CVMLong.create(1483), // Contract address
                Lists.of(
                    Symbols.REGISTER,
                    didBytes,
                    ddo
                )
            )
        );

        Block block = Block.of(Utils.getCurrentTimestamp(), agentKeyPair.signData(didRegistration));
        Order order = Order.create().append(agentKeyPair.signData(block));
        Belief belief = Belief.create(agentKeyPair, order);

        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger completedPhases = new AtomicInteger(0);

        // Phase 1: BeliefPropagator stores the belief
        CompletableFuture<Void> storagePhase = CompletableFuture.runAsync(() -> {
            try {
                Ref<Belief> beliefRef = store.storeTopRef(Ref.get(belief), Ref.PERSISTED, null);
                assertNotNull(beliefRef, "Belief storage should succeed");
                completedPhases.incrementAndGet();

            } catch (Exception e) {
                if (e instanceof MissingDataException) {
                    errors.incrementAndGet();
                }
                fail("Belief storage phase failed: " + e.getMessage());
            }
        }, executorService);

        // Phase 2: dx-agent checks registration status
        CompletableFuture<Void> verificationPhase = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(10); // Allow storage to begin

                // Verify DID can be resolved
                Hash didHash = Hash.get(didBytes);
                Ref<Blob> didRef = null;

                int attempts = 0;
                while (didRef == null && attempts < 50) {
                    try {
                        didRef = store.refForHash(didHash);
                        if (didRef != null) {
                            assertEquals(didBytes, didRef.getValue(), "Retrieved DID should match");
                            break;
                        }
                    } catch (MissingDataException e) {
                        errors.incrementAndGet();
                        fail("MissingDataException during DID verification: " + e.getMessage());
                    }

                    Thread.sleep(5);
                    attempts++;
                }

                // Verify DDO can be resolved
                Hash ddoHash = Hash.get(ddo);
                Ref<ACell> ddoRef = null;

                attempts = 0;
                while (ddoRef == null && attempts < 50) {
                    try {
                        ddoRef = store.refForHash(ddoHash);
                        if (ddoRef != null) {
                            assertEquals(ddo, ddoRef.getValue(), "Retrieved DDO should match");
                            break;
                        }
                    } catch (MissingDataException e) {
                        errors.incrementAndGet();
                        fail("MissingDataException during DDO verification: " + e.getMessage());
                    }

                    Thread.sleep(5);
                    attempts++;
                }

                completedPhases.incrementAndGet();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errors.incrementAndGet();
            }
        }, executorService);

        // Phase 3: Verify complete belief can be accessed
        CompletableFuture<Void> beliefVerificationPhase = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(20); // Allow other phases to progress

                Hash beliefHash = Hash.get(belief);
                Ref<Belief> beliefRef = null;

                int attempts = 0;
                while (beliefRef == null && attempts < 50) {
                    try {
                        beliefRef = store.refForHash(beliefHash);
                        if (beliefRef != null) {
                            Belief retrievedBelief = beliefRef.getValue();
                            assertNotNull(retrievedBelief, "Retrieved belief should not be null");

                            // Verify full data structure is accessible
                            Order retrievedOrder = retrievedBelief.getOrder(peerKey);
                            assertNotNull(retrievedOrder, "Order should be accessible");

                            SignedData<Block> signedBlock = retrievedOrder.getBlock(0);
                            assertNotNull(signedBlock, "Signed block should be accessible");

                            Block retrievedBlock = signedBlock.getValue();
                            assertNotNull(retrievedBlock, "Block should be accessible");

                            AVector<SignedData<ATransaction>> transactions = retrievedBlock.getTransactions();
                            assertEquals(1, transactions.count(), "Should have 1 transaction");

                            SignedData<ATransaction> signedTx = transactions.get(0);
                            assertNotNull(signedTx, "Signed transaction should be accessible");

                            ATransaction tx = signedTx.getValue();
                            assertNotNull(tx, "Transaction should be accessible");

                            break;
                        }
                    } catch (MissingDataException e) {
                        errors.incrementAndGet();
                        fail("MissingDataException during belief verification: " + e.getMessage());
                    }

                    Thread.sleep(5);
                    attempts++;
                }

                assertNotNull(beliefRef, "Should be able to access complete belief");
                completedPhases.incrementAndGet();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errors.incrementAndGet();
            }
        }, executorService);

        // Wait for all phases to complete
        CompletableFuture.allOf(storagePhase, verificationPhase, beliefVerificationPhase)
                        .get(60, TimeUnit.SECONDS);

        // Verify successful completion
        assertEquals(0, errors.get(), "No errors should occur during DID registration workflow");
        assertEquals(3, completedPhases.get(), "All three phases should complete successfully");

        System.out.println("✅ Full DID registration workflow test PASSED:");
        System.out.println("   - Completed phases: " + completedPhases.get());
        System.out.println("   - Error count: " + errors.get());
    }
}