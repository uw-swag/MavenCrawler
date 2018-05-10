package ca.uwaterloo.swag.mavencrawler.db;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.Conventions;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.pojo.Archetype;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;

public class MongoDBHandler {

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
	
	// Default values
	private String host = "localhost";
	private Integer port = 27017;
	private Boolean authEnabled = false;
	private String authDatabase = MONGODB_AUTHDATABASE;
	private String username = "admin";
	private String password = "myPassword";
	private Boolean sslEnabled = false;
	private String replicaSetName = null;
	private String databaseName = "MavenCrawler";
	
	private Logger logger;
	private MongoClient mongo;
	
	private enum PropertyType {
		MONGODB_HOST,
		MONGODB_PORT,
		MONGODB_AUTH_ENABLED,
		MONGODB_AUTHDATABASE,
		MONGODB_USERNAME,
		MONGODB_PASSWORD,
		MONGODB_SSL_ENABLED,
		MONGODB_REPLICASETNAME,
		MONGODB_MAINDATABASE
	}

	// Disable default constructor
	private MongoDBHandler() {}

	public static MongoDBHandler newInstance(Logger aLogger) {
		MongoDBHandler persister = new MongoDBHandler();
		persister.logger = aLogger;
		
		return persister;
	}
	
	public static MongoDBHandler newInstance(Logger aLogger, Properties properties) {
		MongoDBHandler persister = new MongoDBHandler();
		persister.logger = aLogger;
		persister.host = properties.getProperty(PropertyType.MONGODB_HOST.name());
		persister.port = Integer.valueOf(properties.getProperty(PropertyType.MONGODB_PORT.name()));
		persister.authEnabled = Boolean.valueOf(properties.getProperty(PropertyType.MONGODB_AUTH_ENABLED.name()));
		persister.authDatabase = properties.getProperty(PropertyType.MONGODB_AUTHDATABASE.name());
		persister.username = properties.getProperty(PropertyType.MONGODB_USERNAME.name());
		persister.password = properties.getProperty(PropertyType.MONGODB_PASSWORD.name());
		persister.sslEnabled = Boolean.valueOf(properties.getProperty(PropertyType.MONGODB_SSL_ENABLED.name()));
		persister.replicaSetName = properties.getProperty(PropertyType.MONGODB_REPLICASETNAME.name());
		persister.databaseName = properties.getProperty(PropertyType.MONGODB_MAINDATABASE.name());
		
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

	public String getAuthDatabase() {
		return (authDatabase == null) ? MONGODB_AUTHDATABASE : authDatabase;
	}

	public void setAuthDatabase(String authDatabase) {
		this.authDatabase = authDatabase;
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

	public Boolean isSSLEnabled() {
		return sslEnabled;
	}

	public void setSslEnabled(Boolean sslEnabled) {
		this.sslEnabled = sslEnabled;
	}

	public String getReplicaSetName() {
		return replicaSetName;
	}

	public void setReplicaSetName(String replicaSetName) {
		this.replicaSetName = replicaSetName;
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
			return (ping.get("ok").equals(1.0) || ping.get("ok").equals(1));
		} catch (Exception e) {
			LoggerHelper.logError(logger, e, "Could not connect to Mongo Database " + getDatabaseName());
			return false;
		}
	}

	/**
	 * Connects and return the database.
	 * @return the database if connected correctly, otherwise returns null.
	 */
	public MongoDatabase getMongoDatabase() {
		return connect() ? mongo.getDatabase(getDatabaseName()) : null;
	}

	public boolean connect() {
		
		// Don't connect again, if already connected
		if (isConnected()) return true;
		
		boolean success = false;

		// Create Credentials 
		MongoCredential credential = null;
		if (isAuthEnabled()) {
			credential = MongoCredential.createCredential(getUsername(), getAuthDatabase(), getPassword().toCharArray());
		}
		
		// Register classes
		CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClient.getDefaultCodecRegistry(),
				CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).conventions(Conventions.DEFAULT_CONVENTIONS).build()));
		MongoClientOptions options = MongoClientOptions.builder()
				.codecRegistry(pojoCodecRegistry)
				.sslEnabled(sslEnabled)
				.requiredReplicaSetName(replicaSetName)
				.build();
		
		// Creating a Mongo client 
		mongo = (credential == null) ? 
				new MongoClient(new ServerAddress(getHost(), getPort()), options) :
				new MongoClient(new ServerAddress(getHost(), getPort()), credential, options); 
		success = isConnected();
		
		if (success) {
			checkCollectionsIndexes();
			LoggerHelper.log(logger, Level.INFO, "Connected to the database successfully");
		}
		else {
			LoggerHelper.log(logger, Level.WARNING, "Failed connecting to database");
			disconnect();
		}
		
		return success;
	}

	private void checkCollectionsIndexes() {
		MongoDatabase mainDatabase = mongo.getDatabase(getDatabaseName());
		Archetype.checkIndexesInCollection(mainDatabase.getCollection(Archetype.ARCHETYPE_COLLECTION, Archetype.class));
		Metadata.checkIndexesInCollection(mainDatabase.getCollection(Metadata.METADATA_COLLECTION, Metadata.class));
	}

	public boolean disconnect() {

		boolean success = false;
		
		// Close connection
		mongo.close();
		
		try {
			// This should throw an exception when connection is closed
			mongo.getDatabase(getAuthDatabase()).runCommand(new BsonDocument("ping", new BsonInt32(1)));
			LoggerHelper.log(logger, Level.WARNING, "Failed closing connection to database");
		} catch (Exception e) {
			success = true;
		}
		finally {
			mongo = null;
		}
		
		return success;
	}
	
}
