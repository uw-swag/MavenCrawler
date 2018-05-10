package ca.uwaterloo.swag.mavencrawler;

import static ca.uwaterloo.swag.mavencrawler.pojo.Archetype.ARCHETYPE_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.Metadata.METADATA_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.Repository.REPOSITORY_COLLECTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
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

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.pojo.Archetype;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class ArchetypeCrawlerTest {
	
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

		mongoHandler = MongoDBHandler.newInstance(Logger.getLogger(ArchetypeCrawlerTest.class.getName()));
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
		ArchetypeCrawler crawler = new ArchetypeCrawler(logger, handler);
		
		// Then
		assertEquals(logger, crawler.getLogger());
		assertEquals(handler, crawler.getMongoHandler());
	}
	
	@Test
	public void testCrawlXMLInputStream() {
		
		// Given
		Date testStart = new Date();
		ArchetypeCrawler crawler = new ArchetypeCrawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
        String repoURL = "RepoURL";
		
		// When
        crawler.crawlMavenArchetypeXMLInputStream(this.getClass().getResourceAsStream("archetype-catalog-example.xml"), repoURL);
		
        // Then
		Document repository = db.getCollection(REPOSITORY_COLLECTION).find().first();
		List<Document> archetypes = db.getCollection(ARCHETYPE_COLLECTION).find().into(new ArrayList<Document>());

		assertNotNull(repository);
		assertEquals("RepoURL", repository.get("url"));
		Date lastChecked = repository.getDate("lastChecked");
		assertTrue(testStart.before(lastChecked) || testStart.equals(lastChecked));
		assertEquals(2, archetypes.size());
		assertEquals("http://central.maven.org", archetypes.get(0).get("repository"));
		assertEquals(repoURL, archetypes.get(1).get("repository"));
	}
	
	/**
	 * Helper test to crawl from Maven Central
	 */
//	@Test
//	public void testCrawl() throws Exception {
//		
//        // Given
//        ArchetypeCrawler crawler = new ArchetypeCrawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
//        
//        // When
//        crawler.crawlCatalogFromMavenRoot("http://central.maven.org/maven2");
//        
//        // Then
//		MongoCollection<Document> collection = db.getCollection(ARCHETYPE_COLLECTION);
//		System.out.println("Gotten archetypes: " + collection.count());
//		
//		ArrayList<String> docs = collection.distinct("artifactId", String.class).into(new ArrayList<String>());
//		
//		System.out.println("docs: " + docs.size());
//		
//		assertTrue(collection.count() > 0);
//	}

	/**
	 * Testing crawling metadata from archetypes in database
	 * TODO: use mock instead of actual address
	 */
	@Test
	public void testUpdateArchetypes() {
		
		// Given
		Archetype archetype1 = new Archetype();
		Archetype archetype2 = new Archetype();
		archetype1.setGroupId("args4j");
		archetype2.setGroupId("args4j");
		archetype1.setRepository("http://central.maven.org/maven2/");
		archetype2.setRepository("http://central.maven.org/maven2/");
		archetype1.setArtifactId("args4j-tools");
		archetype2.setArtifactId("args4j-site");
		Archetype.upsertInMongo(Arrays.asList(archetype1, archetype2), mongoHandler.getMongoDatabase(), null);
		
		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(2, db.getCollection(ARCHETYPE_COLLECTION).count());
		assertEquals(0, metadataCollection.count());
		
		// When
        ArchetypeCrawler crawler = new ArchetypeCrawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
		crawler.updateArchetypes();
		
		// Then
		assertEquals(2, metadataCollection.count());
	}
	

}
