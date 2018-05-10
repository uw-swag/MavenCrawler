package ca.uwaterloo.swag.mavencrawler;

import java.util.List;
import java.util.logging.Logger;

import com.mongodb.client.MongoDatabase;

import edu.uci.ics.crawler4j.crawler.CrawlController.WebCrawlerFactory;

public class MetadataCrawlerFactory implements WebCrawlerFactory<MetadataCrawler> {
	
	private Logger logger;
	private MongoDatabase mongoDatabase;
	private List<String> seedURLs;

	public MetadataCrawlerFactory(Logger logger, MongoDatabase mongoDatabase, List<String> seedURLs) {
		super();
		this.logger = logger;
		this.mongoDatabase = mongoDatabase;
		this.seedURLs = seedURLs;
	}


	@Override
	public MetadataCrawler newInstance() throws Exception {
		return new MetadataCrawler(logger, mongoDatabase, seedURLs);
	}

}
