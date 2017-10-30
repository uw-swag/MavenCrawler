package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MongoDBPersisterTest {


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
	public void testFactoryMethodShouldNotReturnNull() {
		
		// Given
		Logger aLogger = Logger.getLogger(this.getClass().getName());
		
		// When
		MongoDBPersister persister = MongoDBPersister.newInstance(aLogger);
		
		// Then
		assertNotNull(persister);
	}

	@Test
	public void testConnect() {

		// Given
		// initial parameters
		
		// When
		assertTrue(persister.connect());
		
		// Then
		assertTrue(persister.isConnected());
	}

	@Test
	public void testDisconnect() {

		// Given
		assertTrue(persister.connect());
		
		// When
		assertTrue(persister.disconnect());
		
		// Then
		assertFalse(persister.isConnected());
	}

	
	@Test
	public void testIndexesCreation() {

		// Given
		List<String> collectionNames = Arrays.asList(
				"Archetypes"
				);
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		
		for (String collectionName : collectionNames) {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			List<Document> indexes = collection.listIndexes().into(new ArrayList<Document>());
			assertEquals(0, indexes.size());
		}
		
		// When
		assertTrue(persister.connect());

		// Then
		for (String collectionName : collectionNames) {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			List<Document> indexes = collection.listIndexes().into(new ArrayList<Document>());
			assertTrue(indexes.size() > 0);
		}
	}
	
	@Test
	public void testInsertArchetypes() {

		// Given
		Archetype archetype1 = new Archetype();
		Archetype archetype2 = new Archetype();
		archetype1.setArtifactId("archetype1");
		archetype2.setArtifactId("archetype2");
		
		// When
		persister.upsertArchetypes(Arrays.asList(archetype1, archetype2));
		
		// Then
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Document> collection = db.getCollection("Archetypes");
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(2, documents.size());
		assertEquals("archetype1", documents.get(0).get("artifactId"));
		assertEquals("archetype2", documents.get(1).get("artifactId"));
	}

	
	@Test
	public void testUpdateArchetypes() {

		// Given
		Archetype archetype = new Archetype();
		archetype.setGroupId("group1");
		archetype.setArtifactId("archetype1");
		archetype.setVersion("1");
		persister.upsertArchetypes(Arrays.asList(archetype));

		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		MongoCollection<Document> collection = db.getCollection("Archetypes");
		assertEquals("1", collection.find().first().get("version"));
		
		// When
		archetype.setVersion("2");
		persister.upsertArchetypes(Arrays.asList(archetype));
		
		// Then
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(1, documents.size());
		assertEquals("archetype1", documents.get(0).get("artifactId"));
		assertEquals("group1", documents.get(0).get("groupId"));
		assertEquals("2", documents.get(0).get("version"));
	}

}
