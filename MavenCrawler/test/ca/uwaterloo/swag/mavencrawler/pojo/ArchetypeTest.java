package ca.uwaterloo.swag.mavencrawler.pojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

public class ArchetypeTest {
	
	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private MongodExecutable _mongodExe;
	private MongodProcess _mongod;
	private MongoClient _mongo;
	
	private MongoDBHandler handler;

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
		
		handler = MongoDBHandler.newInstance(Logger.getLogger(this.getClass().getName()));
		handler.setHost("localhost");
		handler.setPort(12345);
		handler.setAuthEnabled(false);
		handler.setDatabaseName("TestDatabase");
		
		_mongo = new MongoClient("localhost", 12345);
	}

	@After
	public void tearDown() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	@Test
	public void testIndexesCreation() throws UnknownHostException, IOException {
		
		// Given
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Archetype> collection = db.getCollection("Archetypes", Archetype.class);
		
		// When
		Archetype.checkIndexesInCollection(collection);

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
	}
	

	@Test
	public void testInsertArchetypes() {

		// Given
		Archetype archetype1 = new Archetype();
		Archetype archetype2 = new Archetype();
		archetype1.setGroupId("group");
		archetype2.setGroupId("group");
		archetype1.setArtifactId("artifact");
		archetype2.setArtifactId("artifact");
		archetype1.setVersion("1");
		archetype2.setVersion("2");

		MongoDatabase db = handler.getMongoDatabase();
		
		// When
		Archetype.upsertInMongo(Arrays.asList(archetype1, archetype2), db, null);
		
		// Then
		MongoCollection<Document> collection = db.getCollection("Archetypes");
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(2, documents.size());
		assertEquals("1", documents.get(0).get("version"));
		assertEquals("2", documents.get(1).get("version"));
	}
	
	@Test
	public void testUpdateArchetypes() {

		// Given
		Archetype archetype = new Archetype();
		archetype.setGroupId("group1");
		archetype.setArtifactId("archetype1");
		archetype.setVersion("1");
		archetype.setDescription("description1");
		
		MongoDatabase db = handler.getMongoDatabase();
		Archetype.upsertInMongo(Arrays.asList(archetype), db, null);
		MongoCollection<Document> collection = db.getCollection("Archetypes");
		assertEquals("1", collection.find().first().get("version"));
		
		// When
		archetype.setDescription("description2");
		Archetype.upsertInMongo(Arrays.asList(archetype), db, null);
		
		// Then
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(1, documents.size());
		assertEquals("archetype1", documents.get(0).get("artifactId"));
		assertEquals("group1", documents.get(0).get("groupId"));
		assertEquals("1", documents.get(0).get("version"));
		assertEquals("description2", documents.get(0).get("description"));
	}

}
