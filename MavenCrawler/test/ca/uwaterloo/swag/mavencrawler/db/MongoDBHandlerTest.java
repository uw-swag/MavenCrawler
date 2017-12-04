package ca.uwaterloo.swag.mavencrawler.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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

public class MongoDBHandlerTest {

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
	public void testFactoryMethodShouldInitializeDefaultValues() {
		
		// Given
		Logger aLogger = Logger.getLogger(this.getClass().getName());
		
		// When
		MongoDBHandler persister = MongoDBHandler.newInstance(aLogger);
		
		// Then
		assertNotNull(persister);
		assertEquals("localhost", persister.getHost());
		assertEquals(27017, persister.getPort().intValue());
		assertFalse(persister.isAuthEnabled());
		assertEquals("admin", persister.getAuthDatabase());
		assertEquals("admin", persister.getUsername());
		assertEquals("myPassword", persister.getPassword());
		assertEquals("MavenCrawler", persister.getDatabaseName());
	}

	@Test
	public void testFactoryMethodWithProperties() throws IOException {
		
		// Given
		Logger aLogger = Logger.getLogger(this.getClass().getName());
		Properties properties = new Properties();
		properties.load(this.getClass().getResourceAsStream("../database-example.conf"));
		
		// When
		MongoDBHandler persister = MongoDBHandler.newInstance(aLogger, properties);
		
		// Then
		assertNotNull(persister);
		assertEquals("192.168.1.1", persister.getHost());
		assertEquals(10000, persister.getPort().intValue());
		assertTrue(persister.isAuthEnabled());
		assertEquals("authdatabase", persister.getAuthDatabase());
		assertEquals("username", persister.getUsername());
		assertEquals("testpassword", persister.getPassword());
		assertTrue(persister.isSSLEnabled());
		assertEquals("replica", persister.getReplicaSetName());
		assertEquals("maindatabase", persister.getDatabaseName());
	}
	
	@Test
	public void testConnect() {

		// Given
		// initial parameters
		
		// When
		assertTrue(handler.connect());
		
		// Then
		assertTrue(handler.isConnected());
	}

	@Test
	public void testDisconnect() {

		// Given
		assertTrue(handler.connect());
		
		// When
		assertTrue(handler.disconnect());
		
		// Then
		assertFalse(handler.isConnected());
	}
	
	@Test
	public void testMongoDatabaseShouldConnect() {
		
		// Given
		assertFalse(handler.isConnected());
		
		// When
		MongoDatabase mongoDatabase = handler.getMongoDatabase();
		
		// Then
		assertTrue(handler.isConnected());
		assertNotNull(mongoDatabase);
	}
	
	@Test
	public void testIndexesCreation() {

		// Given
		List<String> collectionNames = Arrays.asList(
				"Archetypes",
				"Metadata"
				);
		MongoDatabase db = _mongo.getDatabase("TestDatabase");
		
		List<String> dbCollections = db.listCollectionNames().into(new ArrayList<String>());
		assertEquals(0, dbCollections.size());
		
		for (String collectionName : collectionNames) {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			List<Document> indexes = collection.listIndexes().into(new ArrayList<Document>());
			assertEquals(0, indexes.size());
		}
		
		// When
		assertTrue(handler.connect());

		// Then
		dbCollections = db.listCollectionNames().into(new ArrayList<String>());
		assertEquals(collectionNames, dbCollections);
		for (String collectionName : collectionNames) {
			MongoCollection<Document> collection = db.getCollection(collectionName);
			List<Document> indexes = collection.listIndexes().into(new ArrayList<Document>());
			assertTrue(indexes.size() > 0);
		}
	}

}
