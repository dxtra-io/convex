package convex.cli.peer;

import java.io.IOException;
import java.util.List;

import convex.cli.CLIError;
import convex.cli.ExitCodes;
import convex.core.data.AccountKey;
import convex.postgres.PostgresStore;
import convex.peer.API;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Peer genesis command
 */
@Command(
	name = "list",
	mixinStandardHelpOptions = true, 
	description = "List peers in current store.")
public class PeerList extends APeerCommand {

	@ParentCommand
	private Peer peerParent;


	@Override
	public void execute() {
		
		try (PostgresStore store = openStore()) {
			List<AccountKey> keys=API.listPeers(store);
			for (AccountKey k: keys) {
				println(k.toHexString());
			}
		} catch (IOException e) {
			throw new CLIError(ExitCodes.IOERR,"IO Error reading PostgreSQL store: "+e.getMessage(),e);
		}
	}
}
