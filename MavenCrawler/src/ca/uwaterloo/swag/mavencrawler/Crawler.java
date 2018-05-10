package ca.uwaterloo.swag.mavencrawler;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;

public class Crawler {
	
	private Logger logger;
	private MongoDBHandler mongoHandler;

	public Crawler(Logger logger, MongoDBHandler handler) {
		super();
		this.logger = logger;
		this.mongoHandler = handler;
	}

	public Logger getLogger() {
		return logger;
	}
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	public MongoDBHandler getMongoHandler() {
		return mongoHandler;
	}
	public void setMongoHandler(MongoDBHandler mongoHandler) {
		this.mongoHandler = mongoHandler;
	}

	public void crawlMavenURLs(List<String> mavenURLS) {
		logger.log(Level.INFO, "Crawling " + mavenURLS + "...");
		crawlMetadataFromMavenRoots(mavenURLS);
	}

	public void crawlMetadataFromMavenRoots(List<String> mavenRoots) {
		MetadataCrawler.crawlMavenRoots(mavenRoots, mongoHandler.getMongoDatabase(), logger);
	}
	
}
