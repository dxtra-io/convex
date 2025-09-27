package convex.core.lang;

import static convex.test.Assertions.assertCVMEquals;

import org.junit.jupiter.api.Test;

import convex.core.data.ObjectsTest;
import convex.core.data.Syntax;
import convex.store.BaseStoreTest;

import org.junit.jupiter.api.Disabled;

@Disabled
public class SyntaxTest extends BaseStoreTest {

	@Test
	public void testSyntaxConstructor() {
		Syntax s = Syntax.create(RT.cvm(1L));
		assertCVMEquals(1L, s.getValue());

		ObjectsTest.doAnyValueTests(s);
	}
}
