package ca.uwaterloo.swag.mavencrawler;

import static ca.uwaterloo.swag.mavencrawler.pojo.Archetype.ARCHETYPE_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.Metadata.METADATA_COLLECTION;
import static ca.uwaterloo.swag.mavencrawler.pojo.Repository.REPOSITORY_COLLECTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
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
import ca.uwaterloo.swag.mavencrawler.pojo.Downloaded;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;
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
		MongoDBHandler handler = MongoDBHandler.newInstance(logger);
		String folder = "folder";
		
		// When
		Crawler crawler = new Crawler(logger, handler, folder);
		
		// Then
		assertEquals(logger, crawler.getLogger());
		assertEquals(handler, crawler.getMongoHandler());
		assertEquals(folder, crawler.getDownloadFolder());
	}
	
	@Test
	public void testCrawlXMLInputStream() {
		
		// Given
		Date testStart = new Date();
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, null);
        String repoURL = "RepoURL";
		
		// When
        crawler.crawlMavenArchetypeXMLInputStream(this.getClass().getResourceAsStream("archetype-catalog-example.xml"), repoURL);
		
        // Then
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
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
//        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), persister);
//        
//        // When
//        crawler.crawlMavenRoot("http://central.maven.org/maven2");
//        
//        // Then
//		MongoDatabase db = _mongo.getDatabase("TestDatabase");
//		MongoCollection<Document> collection = db.getCollection("Archetypes");
//		System.out.println("Gotten archetypes: " + collection.count());
//		
//		ArrayList<String> docs = collection.distinct("artifactId", String.class).into(new ArrayList<String>());
//		
//		System.out.println("docs: " + docs.size());
//		
//		assertTrue(collection.count() > 0);
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

		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(1, db.getCollection(ARCHETYPE_COLLECTION).count());
		assertEquals(0, metadataCollection.count());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, null);
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

		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(1, db.getCollection(ARCHETYPE_COLLECTION).count());
		assertEquals(0, metadataCollection.count());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, null);
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
		
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Document> metadataCollection = db.getCollection(METADATA_COLLECTION);
		assertEquals(2, db.getCollection(ARCHETYPE_COLLECTION).count());
		assertEquals(0, metadataCollection.count());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, null);
		crawler.updateArchetypes();
		
		// Then
		assertEquals(2, metadataCollection.count());
	}

	/**
	 * Testing downloading libraries
	 * TODO: use mock instead of actual address
	 */
	@Test
	public void testDownloadLibrariesFromMetadata() {
		
		// Given
		Metadata metadata = new Metadata();
		metadata.setGroupId("br.com.ingenieux");
		metadata.setArtifactId("elasticbeanstalk-docker-dropwizard-webapp-archetype");
		metadata.setRepository("http://central.maven.org/maven2");
		metadata.setVersions(Arrays.asList("1.5.0", "1.4.4"));
		
		File rootDownloadFolder = new File("tempDownload");
		File expectedLibraryFolder = new File(rootDownloadFolder, metadata.getGroupId());
		assertFalse(rootDownloadFolder.exists());
		assertFalse(expectedLibraryFolder.exists());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, rootDownloadFolder.getAbsolutePath());
		crawler.downloadLibrariesFromMetadata(metadata);
		
		// Then
		assertTrue(rootDownloadFolder.exists());
		assertTrue(rootDownloadFolder.isDirectory());
		assertTrue(expectedLibraryFolder.exists());
		assertTrue(expectedLibraryFolder.isDirectory());
		assertEquals(1, rootDownloadFolder.list().length);
		assertEquals(2, expectedLibraryFolder.list().length);
		
		File lib1 = new File(expectedLibraryFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.5.0.jar");
		File lib2 = new File(expectedLibraryFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.4.4.jar");
		assertTrue(lib1.exists());
		assertTrue(lib2.exists());

		assertTrue(lib1.delete());
		assertTrue(lib2.delete());
		assertTrue(expectedLibraryFolder.delete());
		assertTrue(rootDownloadFolder.delete());
	}

	/**
	 * Testing downloading libraries
	 * TODO: use mock instead of actual address
	 */
	@Test
	public void testDownloadLibrariesFromMetadataShouldSaveToDB() {
		
		// Given
		Metadata metadata = new Metadata();
		metadata.setGroupId("br.com.ingenieux");
		metadata.setArtifactId("elasticbeanstalk-docker-dropwizard-webapp-archetype");
		metadata.setRepository("http://central.maven.org/maven2");
		metadata.setVersions(Arrays.asList("1.5.0"));
		
		File rootDownloadFolder = new File("tempDownload");
		MongoCollection<Downloaded> collection = mongoHandler.getMongoDatabase().getCollection(Downloaded.DOWNLOADED_COLLECTION, Downloaded.class);
		assertEquals(0, collection.count());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, rootDownloadFolder.getAbsolutePath());
		crawler.downloadLibrariesFromMetadata(metadata);
		
		// Then
		assertEquals(1, collection.count());
		Downloaded downloaded = collection.find().first();
		assertEquals(metadata.getGroupId(), downloaded.getGroupId());
		assertEquals(metadata.getArtifactId(), downloaded.getArtifactId());
		assertEquals(metadata.getRepository(), downloaded.getRepository());
		assertEquals(metadata.getVersions().get(0), downloaded.getVersion());
		assertNotNull(downloaded.getDownloadDate());
		
		File expectedLibPath = new File(rootDownloadFolder, "br.com.ingenieux/br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.5.0.jar");
		assertEquals(expectedLibPath.getAbsolutePath(), downloaded.getDownloadPath());
		
		assertTrue(expectedLibPath.delete());
		assertTrue(expectedLibPath.getParentFile().delete());
		assertTrue(rootDownloadFolder.delete());
	}

	/**
	 * Testing downloading AAR libraries
	 * TODO: use mock instead of actual address
	 */
	@Test
	public void testDownloadAARLibrariesFromMetadata() {
		
		// Given
		Metadata metadata = new Metadata();
		metadata.setGroupId("ch.acra");
		metadata.setArtifactId("acra");
		metadata.setRepository("http://central.maven.org/maven2");
		metadata.setVersions(Arrays.asList("4.9.2"));
		
		File rootDownloadFolder = new File("tempDownload");
		File expectedLibraryFolder = new File(rootDownloadFolder, metadata.getGroupId());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, rootDownloadFolder.getAbsolutePath());
		crawler.downloadLibrariesFromMetadata(metadata);
		
		// Then
		assertTrue(expectedLibraryFolder.exists());
		assertTrue(expectedLibraryFolder.isDirectory());
		assertEquals(1, expectedLibraryFolder.list().length);
		
		File lib1 = new File(expectedLibraryFolder, "ch.acra.acra-4.9.2.aar");
		assertTrue(lib1.exists());

		assertTrue(lib1.delete());
		assertTrue(expectedLibraryFolder.delete());
		assertTrue(rootDownloadFolder.delete());
	}
	
	@Test
	public void testDownloadAllLibraries() {

		// Given
		Metadata metadata1 = new Metadata();
		metadata1.setGroupId("br.com.ingenieux");
		metadata1.setArtifactId("elasticbeanstalk-docker-dropwizard-webapp-archetype");
		metadata1.setRepository("http://central.maven.org/maven2");
		metadata1.setVersions(Arrays.asList("1.5.0"));
		Metadata.upsertInMongo(metadata1, mongoHandler.getMongoDatabase(), null);

		Metadata metadata2 = new Metadata();
		metadata2.setGroupId("args4j");
		metadata2.setArtifactId("args4j-tools");
		metadata2.setRepository("http://central.maven.org/maven2");
		metadata2.setVersions(Arrays.asList("2.33"));
		Metadata.upsertInMongo(metadata2, mongoHandler.getMongoDatabase(), null);
		
		File downloadFolder = new File("tempDownload");
		assertFalse(downloadFolder.exists());
		
		// When
        Crawler crawler = new Crawler(Logger.getLogger(this.getClass().getName()), mongoHandler, downloadFolder.getAbsolutePath());
		crawler.downloadLibraries();
		
		// Then
		assertTrue(downloadFolder.exists());
		assertTrue(downloadFolder.isDirectory());
		assertEquals(2, downloadFolder.list().length);

		File lib1 = new File(downloadFolder, "br.com.ingenieux/br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.5.0.jar");
		File lib2 = new File(downloadFolder, "args4j/args4j.args4j-tools-2.33.jar");
		assertTrue(lib1.exists());
		assertTrue(lib2.exists());

		assertTrue(lib1.delete());
		assertTrue(lib2.delete());
		Arrays.stream(downloadFolder.listFiles()).forEach(f -> assertTrue(f.delete()));
		assertTrue(downloadFolder.delete());
	}
}
