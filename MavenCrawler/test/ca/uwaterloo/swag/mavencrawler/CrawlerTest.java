package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class CrawlerTest {
	
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

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		_mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net("localhost", 12345, Network.localhostIsIPv6()))
				.build());
		_mongod = _mongodExe.start();

		mongoHandler = MongoDBHandler.newInstance(Logger.getLogger(CrawlerTest.class.getName()));
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
	}

	@After
	public void tearDown() throws Exception {
		db.drop();
		db = null;
	}

	@Test
	public void testConstructor() {
		
		// Given
		Logger logger = Logger.getLogger(this.getClass().getName());
		MongoDBHandler handler = MongoDBHandler.newInstance(logger);
		
		// When
		Crawler crawler = new Crawler(logger, handler);
		
		// Then
		assertEquals(logger, crawler.getLogger());
		assertEquals(handler, crawler.getMongoHandler());
	}

	/**
	 * Helper test to crawl multiple metadata from Maven Central
	 * TODO: use mock instead of actual address
	 */
//	@Test
//	public void testCrawlMetadataFromMavenRoots() {
//
//        // Given
//		MongoCollection<Document> collection = db.getCollection(METADATA_COLLECTION);
//		assertEquals(0, collection.count());
//        
//        // When
//		Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, null);
//        crawler.crawlMetadataFromMavenRoots(Arrays.asList("http://central.maven.org/maven2/abbot"));
//        
//        // Then
//		assertTrue(collection.count() > 0);
//		assertEquals(2, collection.count());
//	}
}
