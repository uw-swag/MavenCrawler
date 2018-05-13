package ca.uwaterloo.swag.mavencrawler.pojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
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

public class DownloadedTest {
	
	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private static MongodExecutable _mongodExe;
	private static MongodProcess _mongod;
	private static MongoDBHandler handler;
	
	private MongoDatabase db;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		_mongodExe = starter.prepare(new MongodConfigBuilder()
				.version(Version.Main.PRODUCTION)
				.net(new Net("localhost", 12345, Network.localhostIsIPv6()))
				.build());
		_mongod = _mongodExe.start();
		
		handler = MongoDBHandler.newInstance(Logger.getLogger(ArchetypeTest.class.getName()));
		handler.setHost("localhost");
		handler.setPort(12345);
		handler.setAuthEnabled(false);
		handler.setDatabaseName("TestDatabase");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	@Before
	public void setUp() throws Exception {
		db = handler.getMongoDatabase();
	}

	@After
	public void tearDown() throws Exception {
		db.drop();
		db = null;
	}

	@Test
	public void testIndexesCreation() throws UnknownHostException, IOException {
		
		// Given
		MongoCollection<Metadata> collection = db.getCollection(Downloaded.DOWNLOADED_COLLECTION, Metadata.class);
		
		// When
		Downloaded.checkIndexesInCollection(collection);

		// Then
		List<Document> indexes = collection.listIndexes().into(new ArrayList<Document>());
		assertEquals(2, indexes.size());
		Document idKey = (Document) indexes.get(0).get("key");
		assertNotNull(idKey);
		assertNotNull(idKey.get("_id"));
		Document indexKey = (Document) indexes.get(1).get("key");
		assertNotNull(indexKey);
		assertNotNull(indexKey.get("groupId"));
		assertNotNull(indexKey.get("artifactId"));
		assertNotNull(indexKey.get("repository"));
		assertNotNull(indexKey.get("version"));
	}

	@Test
	public void testDownloadedFieldsConstructor() {

		// Given
		String groupId = "group";
		String artifactId = "artifact";
		String repository = "repository";
		String version = "version";
		Date downloadDate = new Date();
		String downloadPath = "downloadPath";
		
		// When
		Downloaded downloaded = new Downloaded(groupId, artifactId, repository, version, downloadDate, downloadPath);
		
		// Then
		assertEquals(groupId, downloaded.getGroupId());
		assertEquals(artifactId, downloaded.getArtifactId());
		assertEquals(repository, downloaded.getRepository());
		assertEquals(downloadDate, downloaded.getDownloadDate());
		assertEquals(version, downloaded.getVersion());
		assertEquals(downloadPath, downloaded.getDownloadPath());
	}

	@Test
	public void testInsertDownloaded() {

		// Given
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		cal2.add(Calendar.HOUR, 1);
		Downloaded downloaded1 = new Downloaded("groupId", "artifactId", "repo", "1", cal1.getTime(), "path1");
		Downloaded downloaded2 = new Downloaded("groupId", "artifactId", "repo", "2", cal2.getTime(), "path2");

		MongoCollection<Document> collection = db.getCollection(Downloaded.DOWNLOADED_COLLECTION);
		assertEquals(0, collection.count());
		
		// When
		Downloaded.upsertInMongo(downloaded1, db, null);
		Downloaded.upsertInMongo(downloaded2, db, null);
		
		// Then
		assertEquals(2, collection.count());
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(2, documents.size());
		assertEquals("groupId", documents.get(0).get("groupId"));
		assertEquals("artifactId", documents.get(0).get("artifactId"));
		assertEquals("repo", documents.get(0).get("repository"));
		assertEquals("1", documents.get(0).get("version"));
		assertEquals(cal1.getTime(), documents.get(0).get("downloadDate"));
		
		assertEquals("groupId", documents.get(1).get("groupId"));
		assertEquals("artifactId", documents.get(1).get("artifactId"));
		assertEquals("repo", documents.get(1).get("repository"));
		assertEquals("2", documents.get(1).get("version"));
		assertEquals(cal2.getTime(), documents.get(1).get("downloadDate"));
	}

	@Test
	public void testUpsertDownloadedShouldUpdate() {

		// Given
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		cal2.add(Calendar.HOUR, 1);
		Downloaded downloaded1 = new Downloaded("groupId", "artifactId", "repo", "1", cal1.getTime(), "path1");
		Downloaded downloaded2 = new Downloaded("groupId", "artifactId", "repo", "1", cal2.getTime(), "path2");

		MongoCollection<Document> collection = db.getCollection(Downloaded.DOWNLOADED_COLLECTION);
		assertEquals(0, collection.count());

		Downloaded.upsertInMongo(downloaded1, db, null);
		assertEquals(1, collection.count());
		
		Document doc1 = collection.find().first();
		assertEquals("groupId", doc1.get("groupId"));
		assertEquals("artifactId", doc1.get("artifactId"));
		assertEquals("repo", doc1.get("repository"));
		assertEquals("1", doc1.get("version"));
		assertEquals(cal1.getTime(), doc1.get("downloadDate"));
		assertEquals("path1", doc1.get("downloadPath"));

		// When
		Downloaded.upsertInMongo(downloaded2, db, null);
		
		// Then
		assertEquals(1, collection.count());
		
		Document doc2 = collection.find().first();
		assertEquals("groupId", doc2.get("groupId"));
		assertEquals("artifactId", doc2.get("artifactId"));
		assertEquals("repo", doc2.get("repository"));
		assertEquals("1", doc2.get("version"));
		assertEquals(cal2.getTime(), doc2.get("downloadDate"));
		assertEquals("path2", doc2.get("downloadPath"));
	}
	
	@Test
	public void testExists() {

		// Given
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		cal2.add(Calendar.HOUR, 1);
		Downloaded downloaded1 = new Downloaded("groupId", "artifactId", "repo", "1", cal1.getTime(), "path1");
		Downloaded downloaded2 = new Downloaded("groupId", "artifactId", "repo", "2", cal2.getTime(), "path2");

		MongoCollection<Document> collection = db.getCollection(Downloaded.DOWNLOADED_COLLECTION);
		assertEquals(0, collection.count());
		
		// When
		Downloaded.upsertInMongo(downloaded1, db, null);
		assertEquals(1, collection.count());
		
		// Then
		assertTrue(Downloaded.exists(downloaded1, db));
		assertFalse(Downloaded.exists(downloaded2, db));
	}

}
