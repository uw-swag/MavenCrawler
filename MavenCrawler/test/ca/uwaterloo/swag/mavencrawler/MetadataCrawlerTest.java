package ca.uwaterloo.swag.mavencrawler;

import static ca.uwaterloo.swag.mavencrawler.pojo.Metadata.METADATA_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.VersionPom.VERSIONPOM_COLLECTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
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
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.WebURL;

public class MetadataCrawlerTest {

	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private static MongodExecutable _mongodExe;
	private static MongodProcess _mongod;
	private static MongoClient _mongo;
	private static MongoDBHandler mongoHandler;

	private MetadataCrawler crawler;
	private MongoDatabase db;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		_mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net("localhost", 12345, Network.localhostIsIPv6()))
				.build());
		_mongod = _mongodExe.start();

		mongoHandler = MongoDBHandler.newInstance(Logger.getLogger(MetadataCrawlerTest.class.getName()));
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
		this.crawler = new MetadataCrawler(null, mongoHandler.getMongoDatabase());
		db = _mongo.getDatabase("TestDatabase");
	}

	@After
	public void tearDown() throws Exception {
		db.drop();
		db = null;
	}

	@Test
	public void testShouldVisitMustFilterExtensions() {

		// Given
		Page referringPage = null;
		WebURL url = new WebURL();
		
		// When
		List<String> prohibitedURLs = Arrays.asList(
				"http://central.maven.org/file.md5",
				"http://central.maven.org/file.sha1",
				"http://central.maven.org/file.aar",
				"http://central.maven.org/file.jar",
				"http://central.maven.org/file.asc");

		// Then
		for (String prohibitedURL : prohibitedURLs) {
			url.setURL(prohibitedURL);
			assertFalse(crawler.shouldVisit(referringPage, url));
		}
		
		// When
		url.setURL("http://central.maven.org/file.xml");
		
		// Then
		assertTrue(crawler.shouldVisit(referringPage, url));
	}

	@Test
	public void testShouldVisitMustNotCrawlUp() {

		// Given
		WebURL referringUrl = new WebURL();
		referringUrl.setURL("http://central.maven.org/maven2");
		Page referringPage = new Page(referringUrl);
		
		// When
		WebURL url = new WebURL();
		url.setURL("http://central.maven.org");
		
		// Then
		assertTrue(crawler.shouldVisit(null, url));
		assertFalse(crawler.shouldVisit(referringPage, url));
	}

	@Test
	public void testShouldVisitMustNotCrawlCatalog() {

		// Given
		WebURL referringUrl = new WebURL();
		referringUrl.setURL("http://central.maven.org/maven2");
		Page referringPage = new Page(referringUrl);
		
		// When
		WebURL url = new WebURL();
		url.setURL("http://central.maven.org/maven2/archetype-catalog.xml");
		
		// Then
		assertFalse(crawler.shouldVisit(referringPage, url));
	}

	@Test
	public void testShouldVisitMustOnlyCrawlDown() {

		// Given
		WebURL referringUrl = new WebURL();
		referringUrl.setURL("http://central.maven.org/maven2");
		Page referringPage = new Page(referringUrl);
		WebURL url = new WebURL();
		
		// When
		url.setURL("http://central.maven.org/maven2/test");
		
		// Then
		assertTrue(crawler.shouldVisit(referringPage, url));

		// When
		url.setURL("http://central.maven.org/maven/test");
		assertFalse(crawler.shouldVisit(referringPage, url));
	}

	@Test
	public void testShouldVisitPOMFile() {

		// Given
		WebURL referringUrl = new WebURL();
		referringUrl.setURL("http://central.maven.org/maven2/log4j/log4j/1.2.16");
		Page referringPage = new Page(referringUrl);
		WebURL url = new WebURL();
		
		// When
		url.setURL("http://central.maven.org/maven2/log4j/log4j/1.2.16/log4j-1.2.16.pom");
		
		// Then
		assertTrue(crawler.shouldVisit(referringPage, url));
	}

	@Test
	public void testVisitPage() {

		// Given
		WebURL url = new WebURL();
		url.setURL("http://central.maven.org/maven2/activecluster/activecluster/maven-metadata.xml");
		Page page = new Page(url);

		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(0, metadataCollection.count());

		// When
		crawler.visit(page);

		// Then
		assertEquals(1, metadataCollection.count());
		Document metadata = metadataCollection.find().first();
		assertEquals(metadata.get("groupId"), "activecluster");
		assertEquals(metadata.get("repository"), "http://central.maven.org");
	}


	@Test
	public void testVisitPomFile() {

		// Given
		WebURL url = new WebURL();
		url.setURL("http://central.maven.org/maven2/log4j/log4j/1.2.16/log4j-1.2.16.pom");
		Page page = new Page(url);

		MongoCollection<Document> versionPomsCollection = db.getCollection(VERSIONPOM_COLLECTION);
		assertEquals(0, versionPomsCollection.count());

		// When
		crawler.visit(page);

		// Then
		assertEquals(1, versionPomsCollection.count());
		Document metadata = versionPomsCollection.find().first();
		assertEquals(metadata.get("groupId"), "log4j");
		assertEquals(metadata.get("repository"), "http://central.maven.org");
	}

	// JCenter links have a "/:" before folder names (maybe to avoid crawling?)
	@Test
	public void testHandleJCenterLinks() {
		
		// Given
		WebURL url = new WebURL();
		
		// When
		url.setURL("https://jcenter.bintray.com/:AbsFrame/");
		
		// Then
		assertEquals("https://jcenter.bintray.com/AbsFrame/", crawler.handleUrlBeforeProcess(url).getURL());

		
		// When
		url.setURL("http://central.maven.org/maven2/activecluster/activecluster/maven-metadata.xml");
		
		// Then
		assertEquals("http://central.maven.org/maven2/activecluster/activecluster/maven-metadata.xml", crawler.handleUrlBeforeProcess(url).getURL());
	}

	/**
	 * Testing crawling multiple metadata
	 * TODO: use mock instead of actual address
	 */
//	@Test
//	public void testCrawlRepositoryRoot() {
//
//		// Given
//		List<String> mavenRoots = Arrays.asList("http://central.maven.org/maven2/abbot/");
//
//		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
//		assertEquals(0, metadataCollection.count());
//
//		// When
//		MetadataCrawler.crawlMavenRoots(mavenRoots, mongoHandler.getMongoDatabase(), null);
//
//		// Then
//		assertEquals(2, metadataCollection.count());
//	}

	/**
	 * Testing crawling JCenter metadata
	 * TODO: use mock instead of actual address
	 */
//	@Test
//	public void testCrawlJCenterRepository() {
//
//		// Given
//		List<String> mavenRoots = Arrays.asList("https://jcenter.bintray.com/AbsFrame/");
//
//		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
//		assertEquals(0, metadataCollection.count());
//
//		// When
//		MetadataCrawler.crawlMavenRoots(mavenRoots, mongoHandler.getMongoDatabase(), null);
//
//		// Then
//		assertEquals(1, metadataCollection.count());
//	}

}
