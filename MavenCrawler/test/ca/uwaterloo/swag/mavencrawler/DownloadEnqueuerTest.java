package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.db.RabbitMQHandler;
import ca.uwaterloo.swag.mavencrawler.db.RabbitMQHandler.MessageHandler;
import ca.uwaterloo.swag.mavencrawler.db.RabbitMQHandlerTest;
import ca.uwaterloo.swag.mavencrawler.pojo.ArchetypeTest;
import ca.uwaterloo.swag.mavencrawler.pojo.Downloaded;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class DownloadEnqueuerTest {
	
	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private static MongodExecutable _mongodExe;
	private static MongodProcess _mongod;
	private static MongoDBHandler mongoHandler;
	
	private MongoDatabase db;
	private RabbitMQHandler rabbitHandler;
	

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		_mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net("localhost", 12345, Network.localhostIsIPv6()))
				.build());
		_mongod = _mongodExe.start();
		
		mongoHandler = MongoDBHandler.newInstance(Logger.getLogger(ArchetypeTest.class.getName()));
		mongoHandler.setHost("localhost");
		mongoHandler.setPort(12345);
		mongoHandler.setAuthEnabled(false);
		mongoHandler.setDatabaseName("TestDatabase");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	/**
	 * TODO: use mock instead of actual server
	 */
	@Before
	public void setUp() throws Exception {
		rabbitHandler = RabbitMQHandler.newInstance(Logger.getLogger(RabbitMQHandlerTest.class.getName()));
		rabbitHandler.setHost("localhost");
		rabbitHandler.setPort(5672);
		rabbitHandler.setUsername("guest");
		rabbitHandler.setPassword("guest");
		rabbitHandler.setQueueName("test_queue");

		db = mongoHandler.getMongoDatabase();
	}

	@After
	public void tearDown() throws Exception {
		rabbitHandler.disconnect();
		db.drop();
		db = null;
	}

	/**
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testEnqueue() throws InterruptedException {

		// Given
		Metadata metadata1 = new Metadata();
		Metadata metadata2 = new Metadata();
		metadata1.setGroupId("group");
		metadata2.setGroupId("group");
		metadata1.setArtifactId("artifact1");
		metadata2.setArtifactId("artifact2");
		metadata1.setVersions(Arrays.asList("1"));
		metadata2.setVersions(Arrays.asList("2"));
		metadata1.setRepository("repository");
		metadata2.setRepository("repository");
		Metadata.upsertInMongo(metadata1, db, null);
		Metadata.upsertInMongo(metadata2, db, null);
		
		// When
		List<String> expected = Arrays.asList(
				"{\"groupId\":\"group\",\"artifactId\":\"artifact1\",\"repository\":\"repository\",\"version\":\"1\"}",
				"{\"groupId\":\"group\",\"artifactId\":\"artifact2\",\"repository\":\"repository\",\"version\":\"2\"}");
		DownloadEnqueuer.enqueue(mongoHandler, rabbitHandler, null);
		
		List<String> results = new ArrayList<>();
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public boolean handleMessage(String receivedMessage) {
				results.add(receivedMessage);
				return true;
			}; 
		};
		
		rabbitHandler.listenMessages(messageHandler);
		Thread.sleep(200);
		
		// Then
		assertEquals(2, results.size());
		assertEquals(expected, results);
		assertArrayEquals(expected.toArray(), results.toArray());
	}

	/**
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testEnqueueOnlyNotDownloadedYet() throws InterruptedException {

		// Given
		Metadata metadata1 = new Metadata();
		Metadata metadata2 = new Metadata();
		metadata1.setGroupId("group");
		metadata2.setGroupId("group");
		metadata1.setArtifactId("artifact1");
		metadata2.setArtifactId("artifact2");
		metadata1.setVersions(Arrays.asList("1"));
		metadata2.setVersions(Arrays.asList("2"));
		metadata1.setRepository("repository");
		metadata2.setRepository("repository");
		
		Downloaded downloaded = new Downloaded();
		downloaded.setGroupId("group");
		downloaded.setArtifactId("artifact1");
		downloaded.setVersion("1");
		downloaded.setRepository("repository");
		
		Metadata.upsertInMongo(metadata1, db, null);
		Metadata.upsertInMongo(metadata2, db, null);
		Downloaded.upsertInMongo(downloaded, db, null);
		
		// When
		List<String> expected = Arrays.asList(
				"{\"groupId\":\"group\",\"artifactId\":\"artifact2\",\"repository\":\"repository\",\"version\":\"2\"}");
		DownloadEnqueuer.enqueue(mongoHandler, rabbitHandler, null);
		
		List<String> results = new ArrayList<>();
		MessageHandler messageHandler = new MessageHandler() {
			@Override
			public boolean handleMessage(String receivedMessage) {
				results.add(receivedMessage);
				return true;
			}; 
		};
		
		rabbitHandler.listenMessages(messageHandler);
		Thread.sleep(200);
		
		// Then
		assertEquals(1, results.size());
		assertEquals(expected, results);
		assertArrayEquals(expected.toArray(), results.toArray());
	}

}
