package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MetadataCrawlerFactoryTest {

	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private MongodExecutable _mongodExe;
	private MongodProcess _mongod;

	private MongoDBHandler mongoHandler;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {

		_mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net("localhost", 12345, Network.localhostIsIPv6()))
				.build());
		_mongod = _mongodExe.start();

		mongoHandler = MongoDBHandler.newInstance(Logger.getLogger(this.getClass().getName()));
		mongoHandler.setHost("localhost");
		mongoHandler.setPort(12345);
		mongoHandler.setAuthEnabled(false);
		mongoHandler.setDatabaseName("TestDatabase");
	}

	@After
	public void tearDown() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	@Test
	public void testFactory() throws Exception {
		
		// Given
		Logger logger = Logger.getLogger("newLogger");
		MongoDatabase mongoDatabase = mongoHandler.getMongoDatabase();
		String seedURL = "http://seed.com";
		MetadataCrawlerFactory factory = new MetadataCrawlerFactory(logger, mongoDatabase, seedURL);

		// When
		MetadataCrawler crawler = factory.newInstance();
		
		// Then
		assertEquals(logger, crawler.getLogger()); 
		assertEquals(mongoDatabase, crawler.getMongoDatabase()); 
		assertEquals(seedURL, crawler.getSeedURL()); 
	}

}
