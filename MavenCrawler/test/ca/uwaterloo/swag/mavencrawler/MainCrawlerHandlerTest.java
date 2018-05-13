package ca.uwaterloo.swag.mavencrawler;

import static ca.uwaterloo.swag.mavencrawler.pojo.Metadata.METADATA_COLLECTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URISyntaxException;

import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.helpers.TestHelper;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MainCrawlerHandlerTest {
	
	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private static MongodExecutable _mongodExe;
	private static MongodProcess _mongod;
	private static MongoClient _mongo;
	private static MongoDBHandler mongoHandler;
	
	private MongoDatabase db;
	private File tempCrawlerFolder;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		_mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net("localhost", 12345, Network.localhostIsIPv6()))
				.build());
		_mongod = _mongodExe.start();

		mongoHandler = MongoDBHandler.newInstance(null);
		mongoHandler.setHost("localhost");
		mongoHandler.setPort(12345);
		mongoHandler.setAuthEnabled(false);
		mongoHandler.setDatabaseName("TestDatabase");

		_mongo = new MongoClient("localhost", 12345);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	@Before
	public void setUp() throws Exception {
		db = _mongo.getDatabase("TestDatabase");
		tempCrawlerFolder = new File(System.getProperty("user.dir"), "crawlerTemp");
		assertTrue(TestHelper.deleteRecursive(tempCrawlerFolder));
	}

	@After
	public void tearDown() throws Exception {
		db.drop();
		db = null;
		assertTrue(TestHelper.deleteRecursive(tempCrawlerFolder));
	}
	
	/**
	 * Helper test to crawl multiple metadata from Maven Central
	 * TODO: use mock instead of actual address
	 * Uncomment @Test to run it on an actual server.
	 */
//	@Test
	public void testStartCrawling() throws URISyntaxException {

        // Given
		MongoCollection<Document> collection = db.getCollection(METADATA_COLLECTION);
		assertEquals(0, collection.count());
		
		File configFile = new File(this.getClass().getResource("crawler-config-example.conf").toURI());
		File urlsFile = new File(this.getClass().getResource("main-crawler-test-url.list").toURI());
        
        // When
		MainCrawlerHandler.startCrawling(configFile, urlsFile, null);

        // Then
		assertTrue(collection.count() > 0);
		assertEquals(1, collection.count());
	}

}
