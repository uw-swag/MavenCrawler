package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;

public class MavenDownloader {

	private static final String DEFAULT_CONFIG_FILE = "mavencrawler.conf"; 
	private static final String DOWNLOAD_FOLDER_PROPERTY = "DOWNLOAD_FOLDER";

	public static void main(String[] args) throws SecurityException, IOException {
		
		FileHandler fileHandler = new FileHandler("log.txt");
		fileHandler.setFormatter(new SimpleFormatter());
		Logger logger = Logger.getLogger(MainCrawlerHandler.class.getName());
		logger.addHandler(fileHandler);
		
		File configFile = new File(DEFAULT_CONFIG_FILE);
		
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(configFile));
			LoggerHelper.log(logger, Level.INFO, "Read " + configFile.getAbsolutePath());
		} catch (Exception e) {
			LoggerHelper.logError(logger, e, "Could not open mavencrawler.conf file.");
			System.exit(1);
		}
		
		logger.log(Level.INFO, "Downloading libraries...");
		MongoDBHandler persister = MongoDBHandler.newInstance(logger, properties);
		
		Crawler crawler = new Crawler(logger, persister, properties.getProperty(DOWNLOAD_FOLDER_PROPERTY));
		
		while (true) {
			// Keep downloading indefinitely
			crawler.downloadLibraries();
			
			// Wait 5 minutes while more libraries metadata are crawled
			try {
				Thread.sleep(5*60*1000);
			} catch (InterruptedException e) {
				LoggerHelper.log(logger, Level.INFO, "Error with thread interruption while waiting for downloads.");
			}
		}
	}

}
