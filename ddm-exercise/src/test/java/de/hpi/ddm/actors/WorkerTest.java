// e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

package de.hpi.ddm.actors;

import static org.junit.Assert.assertTrue;

import java.time.Duration;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import de.hpi.ddm.configuration.Configuration;
import de.hpi.ddm.singletons.ConfigurationSingleton;
import de.hpi.ddm.systems.MasterSystem;

public class WorkerTest {

	static ActorSystem system;

	@Before
	public void setUp() throws Exception {
		final Configuration c = ConfigurationSingleton.get();
		
		final Config config = ConfigFactory.parseString(
				"akka.remote.artery.canonical.hostname = \"" + c.getHost() + "\"\n" +
				"akka.remote.artery.canonical.port = " + c.getPort() + "\n" +
				"akka.cluster.roles = [" + MasterSystem.MASTER_ROLE + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + c.getActorSystemName() + "@" + c.getMasterHost() + ":" + c.getMasterPort() + "\"]")
			.withFallback(ConfigFactory.load("application"));
		
		system = ActorSystem.create(c.getActorSystemName(), config);
	}

	@After
	public void tearDown() throws Exception {
		TestKit.shutdownActorSystem(system);
	}
	/*
	@Test
	public void testPackageProcessing() {
		// Tests if the message is correctly transmitted, but does not test if the inter-process communication works.
		new TestKit(system) {
			{
				ActorRef worker = system.actorOf(Worker.props(), Worker.DEFAULT_NAME);
				
				within(Duration.ofSeconds(2), () -> {
					// Test if a small message gets passed from one proxy to the other
                    char[] characterSetArray = "ABCDEFGHIJK".toCharArray();
                    Set<Character> characterSet = new HashSet<Character>();
                    for(int i = 0; i < characterSetArray.length; i++) {
                        characterSet.add(characterSetArray[i]);
                    }

                    String work = "1582824a01c4b842e207a51e3cfc47212885e58eb147e33ea29ba212e611904d";
                    Master.WorkPackage workPackage = new Master.WorkPackage(work, (long) 0, 1, false);
                    Worker.WorkPackageMessage message = new Worker.WorkPackageMessage(workPackage, characterSet, 10, new HashSet<Character>());
					
                    Master.ResultMessage resultMessage = new Master.ResultMessage("HJKGDEFBIC", (long) 0, 1, false);

					worker.tell(message, this.getRef()); // Tell the TestActor to send a large message via its large message proxy to the receiver
					this.expectMsg(resultMessage);
					assertTrue(this.getLastSender().equals(worker));
					
					// Will wait for the rest of the within duration
					expectNoMessage();
					return null;
				});
			}
		};
	}
	 */
}
