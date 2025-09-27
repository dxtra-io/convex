package convex.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.awt.GraphicsEnvironment;
import java.awt.HeadlessException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import convex.api.Convex;
import convex.core.State;
import convex.core.crypto.AKeyPair;
import convex.core.exceptions.InvalidDataException;
import convex.core.store.MemoryStore;
import convex.core.store.Stores;
import convex.dlfs.DLFileSystem;
import convex.gui.client.ConvexClient;
import convex.gui.dlfs.DLFSBrowser;
import convex.gui.peer.PeerGUI;
import convex.gui.tools.HackerTools;
import convex.gui.utils.Toolkit;
import convex.peer.PeerException;
import convex.peer.Server;
import convex.postgres.PostgresStore;
import convex.gui.PostgresTestHelper;

/**
 * We can't test much of the GUI easily in unit tests, but we can at least test
 * that the GUI component tree is initialised correctly and consistently.
 */
public class GUITest {
	
	static Server SERVER;
	static Convex CONVEX;
	private static PeerGUI manager = null;
	private static PostgresStore STORE;
	

	static {
		try {
			Toolkit.init();
			STORE = PostgresTestHelper.createStore(true);
			Stores.setGlobalStore(STORE);
			manager=new PeerGUI(3,AKeyPair.generate());
			SERVER=manager.getPrimaryServer();
			CONVEX=Convex.connect(SERVER);
		} catch (HeadlessException e) {
			manager=null;
			if (STORE != null) {
				STORE.close();
				STORE = null;
			}
			Stores.setGlobalStore(new MemoryStore());
		} catch (PeerException e) {
			throw new Error(e);
		}
	}
	
	/**
	 * Test assumption that the GUI can be initialised. If not, we are probably in headless mode, and there is
	 * no point trying to test and GUI components that will just fail with HeadlessException
	 */
	public static void assumeGUI() {
		assumeTrue(manager!=null);
		assumeFalse(GraphicsEnvironment.isHeadless());
	}
	
	/**
	 * Manager is the root panel of the GUI. A lot of other stuff is built in its
	 * constructor.
	 */

	@Test
	public void testState() throws InvalidDataException {
		GUITest.assumeGUI();
		
		State s = manager.getLatestState();
		s.validate();
	}
	
	/**
	 * Simple test that DLFSBrowser can be constructed and functionality is working
	 */
	@Test
	public void testDLFSBrowser() {
		GUITest.assumeGUI();
		
		DLFSBrowser browser=new DLFSBrowser();
		DLFileSystem drive=browser.getDrive();
		assertEquals(0,drive.getRoot().getNameCount());
	}
	
	@Test
	public void testHackerTools() {
		GUITest.assumeGUI();
		
		HackerTools tools=new HackerTools();
		assertNotNull(tools.tabs);
	}
	
	@Test
	public void testClientTools() {
		GUITest.assumeGUI();
		
		ConvexClient client=new ConvexClient(CONVEX);
		assertNotNull(client.tabs);
	}

	@AfterAll
	public static void shutdownPeers() {
		if (manager!=null) {
			manager.close();
		}
		if (SERVER!=null) {
			SERVER.shutdown();
		}
		if (STORE!=null) {
			STORE.close();
		}
		Stores.setGlobalStore(new MemoryStore());
	}
}
