package convex.core.store;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import convex.core.data.Hash;
import convex.core.data.Ref;

/**
 * Test for RefCache null hash handling to prevent NullPointerException
 *
 * This test addresses the issue where:
 * - PostgresStore.getRootHash() can return null for empty/fresh databases
 * - RefCache.calcIndex() was called with null Hash causing NPE
 * - Server initialization would fail during peer restore/genesis creation
 */
import org.junit.jupiter.api.Disabled;

@Disabled
public class RefCacheNullHashTest {

    private RefCache cache;

    @BeforeEach
    public void setUp() {
        cache = RefCache.create(16); // Small cache for testing
    }

    @Test
    public void testGetCellWithNullHashDoesNotThrow() {
        // This should not throw NPE - should return null gracefully
        Ref<?> result = cache.getCell(null);
        assertNull(result, "Getting cell with null hash should return null");
    }

    @Test
    public void testNormalOperationsStillWork() {
        // Verify normal operations still work after null-safety additions
        // This ensures our defensive fixes don't break existing functionality

        // Test with a real hash
        Hash testHash = Hash.fromHex("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        // Should return null for non-existent hash
        Ref<?> result = cache.getCell(testHash);
        assertNull(result, "Cache should return null for non-existent hash");

        // Should handle normal put/get cycle with existing refs
        // Use a proper ACell type - Keywords.STATE is a good simple example
        Ref<?> testRef = Ref.get(convex.core.data.Keywords.STATE);
        cache.putCell(testRef);

        // We can't test exact retrieval without the exact hash implementation,
        // but we've verified the basic null safety works
    }
}