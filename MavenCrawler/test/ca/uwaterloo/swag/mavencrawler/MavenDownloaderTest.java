package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.pojo.Downloaded;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MavenDownloaderTest {
	
	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private static MongodExecutable _mongodExe;
	private static MongodProcess _mongod;
	private static MongoClient _mongo;
	private static MongoDBHandler mongoHandler;
	
	private File downloadFolder;
	private MongoDatabase db;
	private MavenDownloader downloader;

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
		downloadFolder = new File("tempDownload");
		assertFalse(downloadFolder.exists());
		downloader = new MavenDownloader(Logger.getLogger(this.getClass().getName()), mongoHandler, downloadFolder.getAbsolutePath());
	}

	@After
	public void tearDown() throws Exception {
		db.drop();
		db = null;
		if (downloadFolder.exists()) {
			assertTrue(deleteRecursive(downloadFolder));
		}
	}

	private boolean deleteRecursive(File file) {
		
		// Delete children inside folder
		if (file.exists() && file.isDirectory()) {
			Arrays.stream(file.listFiles()).forEach(f -> assertTrue(deleteRecursive(f)));
		}
		
		// Delete empty folder and/or file
		if (file.exists()) {
			return file.delete();
		}
		
		return false;
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
		
		File expectedLibraryFolder = new File(downloadFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype");
		assertFalse(expectedLibraryFolder.exists());
		
		// When
		downloader.downloadLibrariesFromMetadata(metadata);
		
		// Then
		assertTrue(downloadFolder.exists());
		assertTrue(downloadFolder.isDirectory());
		assertTrue(expectedLibraryFolder.exists());
		assertTrue(expectedLibraryFolder.isDirectory());
		assertEquals(1, downloadFolder.list().length);
		assertEquals(2, expectedLibraryFolder.list().length);
		
		File lib1 = new File(expectedLibraryFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.5.0.jar");
		File lib2 = new File(expectedLibraryFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.4.4.jar");
		assertTrue(lib1.exists());
		assertTrue(lib2.exists());
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
		downloader.downloadLibrariesFromMetadata(metadata);
		
		// Then
		assertEquals(1, collection.count());
		Downloaded downloaded = collection.find().first();
		assertEquals(metadata.getGroupId(), downloaded.getGroupId());
		assertEquals(metadata.getArtifactId(), downloaded.getArtifactId());
		assertEquals(metadata.getRepository(), downloaded.getRepository());
		assertEquals(metadata.getVersions().get(0), downloaded.getVersion());
		assertNotNull(downloaded.getDownloadDate());
		
		File expectedLibPath = new File(rootDownloadFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype/br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.5.0.jar");
		assertEquals(expectedLibPath.getAbsolutePath(), downloaded.getDownloadPath());
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
		File expectedLibraryFolder = new File(rootDownloadFolder, "ch.acra.acra");
		
		// When
		downloader.downloadLibrariesFromMetadata(metadata);
		
		// Then
		assertTrue(expectedLibraryFolder.exists());
		assertTrue(expectedLibraryFolder.isDirectory());
		assertEquals(1, expectedLibraryFolder.list().length);
		
		File lib1 = new File(expectedLibraryFolder, "ch.acra.acra-4.9.2.aar");
		assertTrue(lib1.exists());
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
		
		downloadFolder = new File("tempDownload");
		
		// When
		downloader.downloadLibraries();
		
		// Then
		assertTrue(downloadFolder.exists());
		assertTrue(downloadFolder.isDirectory());
		assertEquals(2, downloadFolder.list().length);

		File lib1 = new File(downloadFolder, "br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype/br.com.ingenieux.elasticbeanstalk-docker-dropwizard-webapp-archetype-1.5.0.jar");
		File lib2 = new File(downloadFolder, "args4j.args4j-tools/args4j.args4j-tools-2.33.jar");
		assertTrue(lib1.exists());
		assertTrue(lib2.exists());

		assertTrue(lib1.delete());
		assertTrue(lib2.delete());
		Arrays.stream(downloadFolder.listFiles()).forEach(f -> assertTrue(f.delete()));
		assertTrue(downloadFolder.delete());
	}

}
