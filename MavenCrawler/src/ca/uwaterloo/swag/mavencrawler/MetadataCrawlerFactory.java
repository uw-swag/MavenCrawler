package ca.uwaterloo.swag.mavencrawler;

import java.util.logging.Logger;

import com.mongodb.client.MongoDatabase;

import edu.uci.ics.crawler4j.crawler.CrawlController.WebCrawlerFactory;

public class MetadataCrawlerFactory implements WebCrawlerFactory<MetadataCrawler> {
	
	private Logger logger;
	private MongoDatabase mongoDatabase;
	private String seedURL;

	public MetadataCrawlerFactory(Logger logger, MongoDatabase mongoDatabase, String seedURL) {
		super();
		this.logger = logger;
		this.mongoDatabase = mongoDatabase;
		this.seedURL = seedURL;
	}


	@Override
	public MetadataCrawler newInstance() throws Exception {
		return new MetadataCrawler(logger, mongoDatabase, seedURL);
	}

}
