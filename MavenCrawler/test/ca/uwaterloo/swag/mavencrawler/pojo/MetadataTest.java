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

public class MetadataTest {
	
	/**
	 * please store Starter or RuntimeConfig in a static final field
	 * if you want to use artifact store caching (or else disable caching)
	 */
	private static final MongodStarter starter = MongodStarter.getDefaultInstance();
	private MongodExecutable _mongodExe;
	private MongodProcess _mongod;
	
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
	}

	@After
	public void tearDown() throws Exception {
		_mongod.stop();
		_mongodExe.stop();
	}

	@Test
	public void testIndexesCreation() throws UnknownHostException, IOException {
		
		// Given
		MongoDatabase db = handler.getMongoDatabase();
		MongoCollection<Metadata> collection = db.getCollection("Metadata", Metadata.class);
		
		// When
		Metadata.checkIndexesInCollection(collection);

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
	}

	@Test
	public void testInsertMetadata() {

		// Given
		Metadata metadata1 = new Metadata();
		Metadata metadata2 = new Metadata();
		metadata1.setGroupId("group");
		metadata2.setGroupId("group");
		metadata1.setArtifactId("artifact1");
		metadata2.setArtifactId("artifact2");
		metadata1.setVersions(Arrays.asList("1"));
		metadata2.setVersions(Arrays.asList("2"));

		MongoDatabase db = handler.getMongoDatabase();
		
		// When
		Metadata.upsertInMongo(metadata1, db, null);
		Metadata.upsertInMongo(metadata2, db, null);
		
		// Then
		MongoCollection<Document> collection = db.getCollection("Metadata");
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(2, documents.size());
		assertEquals(Arrays.asList("1"), documents.get(0).get("versions"));
		assertEquals(Arrays.asList("2"), documents.get(1).get("versions"));
	}

	@Test
	public void testUpdateMetadata() {

		// Given
		Metadata metadata1 = new Metadata();
		Metadata metadata2 = new Metadata();
		metadata1.setGroupId("group");
		metadata2.setGroupId("group");
		metadata1.setArtifactId("artifact");
		metadata2.setArtifactId("artifact");
		metadata1.setVersions(Arrays.asList("1", "2"));
		metadata2.setVersions(Arrays.asList("2", "3"));

		MongoDatabase db = handler.getMongoDatabase();
		MongoCollection<Document> collection = db.getCollection("Metadata");
		Metadata.upsertInMongo(metadata1, db, null);
		assertEquals(1, collection.count());
		
		// When
		Metadata.upsertInMongo(metadata2, db, null);
		
		// Then
		ArrayList<Document> documents = collection.find().into(new ArrayList<Document>());
		
		assertNotNull(documents);
		assertEquals(1, documents.size());
		assertEquals(Arrays.asList("1", "2", "3"), documents.get(0).get("versions"));
	}

}
