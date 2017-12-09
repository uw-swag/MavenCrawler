package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.helpers.StringHelper;

public class MainCrawlerHandler {
	
	private static final String DEFAULT_CONFIG_FILE = "mavencrawler.conf"; 
	private static final String DEFAULT_URLS_LIST = "mavenURLs.list"; 
	private static final String DOWNLOAD_FOLDER_PROPERTY = "DOWNLOAD_FOLDER";

	public static void main(String[] args) {
		
		Logger logger = Logger.getLogger(MainCrawlerHandler.class.getName());
		File configFile = new File(DEFAULT_CONFIG_FILE);
		File urlsFile = new File(DEFAULT_URLS_LIST);
		
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(configFile));
			logger.log(Level.INFO, "Read " + configFile.getAbsolutePath());
		} catch (Exception e) {
			LoggerHelper.logError(logger, e, "Could not open mavencrawler.conf file.");
			System.exit(1);
		}
		
		List<String> mavenURLs = StringHelper.getStringsFromFile(urlsFile.getAbsolutePath());
		logger.log(Level.INFO, "Crawling " + mavenURLs.size() + " maven URLs.");
		MongoDBHandler persister = MongoDBHandler.newInstance(logger, properties);
		Crawler crawler = new Crawler(logger, persister, properties.getProperty(DOWNLOAD_FOLDER_PROPERTY));
		crawler.crawlMavenURLs(mavenURLs);
	}

}
