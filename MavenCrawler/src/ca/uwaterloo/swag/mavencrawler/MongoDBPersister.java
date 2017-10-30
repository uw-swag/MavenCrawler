package ca.uwaterloo.swag.mavencrawler;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;

public class MongoDBPersister {

	/**
	 * MongoDB reference guides
	 * Java Driver: https://mongodb.github.io/mongo-java-driver/
	 * Java Quick Start: http://mongodb.github.io/mongo-java-driver/3.5/driver/getting-started/quick-start/
	 * Java Tutorial: https://www.tutorialspoint.com/mongodb/mongodb_java.htm
	 * Java Driver Documentation: http://mongodb.github.io/mongo-java-driver/3.5/driver/
	 * MongoDB Installation: https://docs.mongodb.com/manual/administration/install-community/
	 * MongoDB Authentication: https://docs.mongodb.com/manual/core/security-users/
	 * How to create user: https://stackoverflow.com/questions/4881208/how-to-secure-mongodb-with-username-and-password
	 * 
	 * Quick commands:
	 * Run MongoDB: mongod --dbpath path/to/data/db/
	 * Run MongoDB with authentication: mongod --dbpath path/to/data/db/ --auth
	 * Create user in MongoDB bash:
	 * > use admin
	 * > db.createUser({ user: "username", pwd: "password", roles: [ "root" ] })
	 */
	
	private static final String MONGODB_AUTHDATABASE = "admin";
	private static final String ARCHETYPE_COLLECTION = "Archetypes";
	private String host;
	private Integer port;
	private Boolean authEnabled;
	private String username;
	private String password;
	private String authDatabase;
	private String databaseName;
	private Logger logger;
	private MongoClient mongo;


	// Disable default constructor
	private MongoDBPersister() {}

	public static MongoDBPersister newInstance(Logger aLogger) {
		MongoDBPersister persister = new MongoDBPersister();
		persister.logger = aLogger;
		
		return persister;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public Boolean isAuthEnabled() {
		return authEnabled;
	}

	public void setAuthEnabled(Boolean authEnabled) {
		this.authEnabled = authEnabled;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getAuthDatabase() {
		return (authDatabase == null) ? MONGODB_AUTHDATABASE : authDatabase;
	}

	public void setAuthDatabase(String authDatabase) {
		this.authDatabase = authDatabase;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * Checks if it's currently connected to the database.
	 * @return the connection status TRUE if connected correctly.
	 */
	public boolean isConnected() {
		
		if (mongo == null) return false;
		
		try {
			Document ping = mongo.getDatabase(getDatabaseName()).runCommand(new BsonDocument("ping", new BsonInt32(1)));
			return ping.get("ok").equals(1.0);
		} catch (Exception e) {
			logError(e, "Could not connect to Mongo Database " + getDatabaseName());
			return false;
		}
	}

	public boolean connect() {
		
		// Don't connect again, if already connected
		if (isConnected()) return true;
		
		boolean success = false;

		// Create Credentials 
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		if (isAuthEnabled()) {
			credentials.add(MongoCredential.createCredential(getUsername(), getAuthDatabase(), getPassword().toCharArray()));
		}
		
		// Register classes
		CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(),
				CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		MongoClientOptions options = MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build();
		
		// Creating a Mongo client 
		mongo = new MongoClient(new ServerAddress(getHost(), getPort()), credentials, options); 
		success = isConnected();
		
		if (success) {
			checkCollectionsIndexes();
			logger.log(Level.INFO, "Connected to the database successfully");
		}
		else {
			logger.log(Level.WARNING, "Failed connecting to database");
			disconnect();
		}
		
		return success;
	}

	private void checkCollectionsIndexes() {
		MongoDatabase mainDatabase = mongo.getDatabase(getDatabaseName());
		Archetype.checkIndexesInCollection(mainDatabase.getCollection(ARCHETYPE_COLLECTION, Archetype.class));
	}

	public boolean disconnect() {

		boolean success = false;
		
		// Close connection
		mongo.close();
		
		try {
			// This should throw an exception when connection is closed
			mongo.getDatabase(getAuthDatabase()).runCommand(new BsonDocument("ping", new BsonInt32(1)));
			logger.log(Level.WARNING, "Failed closing connection to database");
		} catch (Exception e) {
			success = true;
		}
		finally {
			mongo = null;
		}
		
		return success;
	}

	public void upsertArchetypes(List<Archetype> archetypesList) {
		
		if (!connect()) return;
		
		List<UpdateOneModel<Archetype>> upsertRequests = new ArrayList<UpdateOneModel<Archetype>>(archetypesList.size());
		UpdateOptions updateOptions = new UpdateOptions().upsert(true);
		
		for (Archetype archetype : archetypesList) {
			upsertRequests.add(new UpdateOneModel<Archetype>(
					and(eq("groupId", archetype.getGroupId()), eq("artifactId", archetype.getArtifactId())), 
					new Document("$set", archetype), 
					updateOptions));
		}

		MongoCollection<Archetype> collection = mongo.getDatabase(getDatabaseName()).getCollection(ARCHETYPE_COLLECTION, Archetype.class);
		collection.bulkWrite(upsertRequests);
	}

	// Helpers
	private void logError(Exception e, String message) {
		e.printStackTrace();
		String theError = message;
		if (e.getStackTrace().length>=1){
			theError += "\n" + e.getStackTrace()[0].toString();
		}
		logger.log(Level.SEVERE, theError);
	}
	
}
