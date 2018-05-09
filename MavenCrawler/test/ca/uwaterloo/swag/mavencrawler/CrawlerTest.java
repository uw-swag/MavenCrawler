package ca.uwaterloo.swag.mavencrawler;

import static ca.uwaterloo.swag.mavencrawler.pojo.Archetype.ARCHETYPE_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.Metadata.METADATA_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.Repository.REPOSITORY_COLLECTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
	
	@Test
	public void testCrawlXMLInputStream() {
		
		// Given
		Date testStart = new Date();
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
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
//        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, null);
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

	/**
	 * Testing crawling metadata
	 * TODO: use mock instead of actual address
	 */
	@Test
	public void testCrawlMetadata() {
		
		// Given
		Archetype archetype = new Archetype();
		archetype.setGroupId("args4j");
		archetype.setArtifactId("args4j-tools");
		archetype.setRepository("http://central.maven.org/maven2/");
		Archetype.upsertInMongo(Arrays.asList(archetype), mongoHandler.getMongoDatabase(), null);

		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(1, db.getCollection(ARCHETYPE_COLLECTION).count());
		assertEquals(0, metadataCollection.count());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
		crawler.updateMetadataForArchetype(archetype);
		
		// Then
		assertEquals(1, metadataCollection.count());
		Document metadata = metadataCollection.find().first();
		assertEquals("args4j", metadata.get("groupId"));
		assertEquals("args4j-tools", metadata.get("artifactId"));
		assertEquals("http://central.maven.org/maven2/", metadata.get("repository"));
		assertEquals("2.33", metadata.get("latest"));
		assertEquals("2.33", metadata.get("release"));
		assertEquals(11, ((List<?>)metadata.get("versions")).size());
		
		Calendar cal = Calendar.getInstance();
		cal.set(2016, Calendar.JANUARY, 31, 9, 2, 3);
		cal.set(Calendar.MILLISECOND, 0);
		assertEquals(cal.getTime(), metadata.get("lastUpdated"));
	}

	/**
	 * Testing crawling metadata
	 * TODO: use mock instead of actual address
	 */
	@Test
	public void testCrawlMetadataWithSubGroups() {
		
		// Given
		Archetype archetype = new Archetype();
		archetype.setGroupId("am.ik.archetype");
		archetype.setArtifactId("maven-reactjs-blank-archetype");
		archetype.setRepository("http://central.maven.org/maven2/");
		Archetype.upsertInMongo(Arrays.asList(archetype), mongoHandler.getMongoDatabase(), null);

		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(1, db.getCollection(ARCHETYPE_COLLECTION).count());
		assertEquals(0, metadataCollection.count());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
		crawler.updateMetadataForArchetype(archetype);
		
		// Then
		assertEquals(1, metadataCollection.count());
		Document metadata = metadataCollection.find().first();
		assertEquals("am.ik.archetype", metadata.get("groupId"));
		assertEquals("maven-reactjs-blank-archetype", metadata.get("artifactId"));
		assertEquals("http://central.maven.org/maven2/", metadata.get("repository"));
		assertEquals("1.0.0", metadata.get("latest"));
		assertEquals("1.0.0", metadata.get("release"));
		assertEquals(1, ((List<?>)metadata.get("versions")).size());
		
		Calendar cal = Calendar.getInstance();
		cal.set(2015, Calendar.MARCH, 23, 16, 58, 46);
		cal.set(Calendar.MILLISECOND, 0);
		assertEquals(cal.getTime(), metadata.get("lastUpdated"));
	}

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
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler);
		crawler.updateArchetypes();
		
		// Then
		assertEquals(2, metadataCollection.count());
	}
}
