package ca.uwaterloo.swag.mavencrawler.pojo;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class VersionPomTest {
	
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
		
		handler = MongoDBHandler.newInstance(Logger.getLogger(VersionPomTest.class.getName()));
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
		MongoCollection<VersionPom> collection = db.getCollection("VersionPoms", VersionPom.class);
		
		// When
		VersionPom.checkIndexesInCollection(collection);

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
		assertNotNull(indexKey.get("version"));
		assertNotNull(indexKey.get("repository"));
	}

	@Test
	public void testInsertVersionPoms() {

		// Given
		VersionPom versionPom1 = new VersionPom();
		VersionPom versionPom2 = new VersionPom();
		versionPom1.setGroupId("group");
		versionPom2.setGroupId("group");
		versionPom1.setArtifactId("artifact");
		versionPom2.setArtifactId("artifact");
		versionPom1.setVersion("1");
		versionPom2.setVersion("2");
		versionPom1.setRepository("http://central.maven.org/maven2");
		versionPom2.setRepository("http://central.maven.org/maven2");
		
		// When
		VersionPom.upsertInMongo(Arrays.asList(versionPom1, versionPom2), db, null);
		
		// Then
		MongoCollection<Document> collection = db.getCollection("VersionPoms");
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(2, documents.size());
		assertEquals("1", documents.get(0).get("version"));
		assertEquals("2", documents.get(1).get("version"));
	}
	
	@Test
	public void testUpdateVersionPoms() {

		// Given
		VersionPom versionPom = new VersionPom();
		versionPom.setGroupId("group1");
		versionPom.setArtifactId("archetype1");
		versionPom.setVersion("1");
		versionPom.setRepository("repository1");
		versionPom.setDescription("description1");
		
		VersionPom.upsertInMongo(Arrays.asList(versionPom), db, null);
		MongoCollection<Document> collection = db.getCollection("VersionPoms");
		assertEquals("1", collection.find().first().get("version"));
		
		// When
		versionPom.setDescription("description2");
		VersionPom.upsertInMongo(Arrays.asList(versionPom), db, null);
		
		// Then
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(1, documents.size());
		assertEquals("archetype1", documents.get(0).get("artifactId"));
		assertEquals("group1", documents.get(0).get("groupId"));
		assertEquals("1", documents.get(0).get("version"));
		assertEquals("repository1", documents.get(0).get("repository"));
		assertEquals("description2", documents.get(0).get("description"));
	}
	
	@Test
	public void testFindAllVersionPoms() {
		
		// Given
		VersionPom versionPom1 = new VersionPom();
		VersionPom versionPom2 = new VersionPom();
		versionPom1.setGroupId("group");
		versionPom2.setGroupId("group");
		versionPom1.setArtifactId("artifact");
		versionPom2.setArtifactId("artifact");
		versionPom1.setVersion("1");
		versionPom2.setVersion("2");
		versionPom1.setRepository("http://central.maven.org/maven2");
		versionPom2.setRepository("http://central.maven.org/maven2");

		VersionPom.upsertInMongo(Arrays.asList(versionPom1, versionPom2), db, null);
		
		// When
		List<VersionPom> versionPoms = VersionPom.findAllFromMongo(db);
		
		// Then
		assertEquals(2, versionPoms.size());
	}

}
