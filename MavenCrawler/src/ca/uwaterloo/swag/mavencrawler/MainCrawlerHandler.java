package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBPersister;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.helpers.StringHelper;

public class MainCrawlerHandler {
	
	private static final String DEFAULT_DATABASE_CONFIG = "database.conf"; 
	private static final String DEFAULT_URLS_LIST = "mavenURLs.list"; 

	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(MainCrawlerHandler.class.getName());
		File configFile = new File(DEFAULT_DATABASE_CONFIG);
		File urlsFile = new File(DEFAULT_URLS_LIST);
		
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(configFile));
			logger.log(Level.INFO, "Read " + configFile.getAbsolutePath());
		} catch (Exception e) {
			LoggerHelper.logError(logger, e, "Could not open database.conf file.");
			System.exit(1);
		}
		
		List<String> mavenURLs = StringHelper.getStringsFromFile(urlsFile.getAbsolutePath());
		logger.log(Level.INFO, "Crawling " + mavenURLs.size() + " maven URLs.");
		MongoDBPersister persister = MongoDBPersister.newInstance(logger, properties);
		Crawler crawler = new Crawler(logger, persister);
		crawler.crawlMavenURLs(mavenURLs);
	}

}
