package ca.uwaterloo.swag.mavencrawler.pojo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Date;
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

public class RepositoryTest {
	
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
	public void testGetLastCheckedDateShouldInsertNotFoundURL() {
		
		// Given
		MongoDatabase db = handler.getMongoDatabase();
		assertEquals(0, db.getCollection("Repositories").count());
		String url = "url";
		
		// When
		Date date = Repository.getLastCheckedDateForURLInMongo(url, db);
		
		// Then
		assertNull(date);
		Document document = db.getCollection("Repositories").find().first();
		assertNotNull(document);
		assertEquals(url, document.get("url"));
	}

	@Test
	public void testGetLastCheckedDateShouldNotDuplicateURLs() {
		
		// Given
		String url = "url";
		Repository repo = new Repository();
		repo.setUrl(url);
		repo.setLastChecked(new Date());
		MongoCollection<Repository> collection = db.getCollection("Repositories", Repository.class);
		collection.insertOne(repo);
		assertEquals(1, db.getCollection("Repositories").count());
		
		// When
		Date date = Repository.getLastCheckedDateForURLInMongo(url, db);

		// Then
		assertEquals(repo.getLastChecked(), date);
		assertEquals(1, db.getCollection("Repositories").count());
	}
	
	@Test
	public void testSetLastCheckedDateShouldInsertNotFoundURL() {

		// Given
		assertEquals(0, db.getCollection("Repositories").count());
		String url = "url";
		
		// When
		Date date = new Date();
		Repository.setLastCheckedDateForURLInMongo(url, db, date);
		
		// Then
		Document document = db.getCollection("Repositories").find().first();
		assertNotNull(document);
		assertEquals(url, document.get("url"));
		assertEquals(date, document.get("lastChecked"));
	}

}
