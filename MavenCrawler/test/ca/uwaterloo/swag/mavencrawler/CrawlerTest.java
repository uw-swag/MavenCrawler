package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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
	private MongodExecutable _mongodExe;
	private MongodProcess _mongod;
	private MongoClient _mongo;
	
	private MongoDBPersister persister;

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
			
			persister = MongoDBPersister.newInstance(Logger.getLogger(this.getClass().getName()));
			persister.setHost("localhost");
			persister.setPort(12345);
			persister.setAuthEnabled(false);
			persister.setDatabaseName("TestDatabase");

			_mongo = new MongoClient("localhost", 12345);
	}

	@After
	public void tearDown() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	@Test
	public void testConstructor() {
		
		// Given
		Logger logger = Logger.getLogger(this.getClass().getName());
		MongoDBPersister persister = MongoDBPersister.newInstance(logger);
		
		// When
		Crawler crawler = new Crawler(logger, persister);
		
		// Then
		assertEquals(logger, crawler.getLogger());
		assertEquals(persister, crawler.getPersister());
	}
	
	@Test
	public void testCrawlXMLInputStream() {
		
		// Given
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), persister);
		
		// When
        crawler.crawlMavenArchetypeXMLInputStream(this.getClass().getResourceAsStream("archetype-catalog-example.xml"));
		
        // Then
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Document> collection = db.getCollection("Archetypes");
		assertEquals(2, collection.count());
	}
	
	/**
	 * Helper test to crawl from Maven Central
	 */
//	@Test
//	public void testCrawl() throws Exception {
//		
//        // Given
//        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), persister);
//        
//        // When
//        crawler.crawlMavenRoot("http://central.maven.org/maven2");
//        
//        // Then
//		MongoDatabase db = _mongo.getDatabase("TestDatabase");
//		MongoCollection<Document> collection = db.getCollection("Archetypes");
//		System.out.println("Gotten archetypes: " + collection.count());
//		assertTrue(collection.count() > 0);
//	}

}
