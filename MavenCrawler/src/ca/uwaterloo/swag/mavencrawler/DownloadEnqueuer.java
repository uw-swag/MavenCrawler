package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.db.RabbitMQHandler;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.pojo.Downloaded;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;

public class DownloadEnqueuer {

	private static final String DEFAULT_CONFIG_FILE = "mavencrawler.conf"; 

	public static void main(String[] args) throws InterruptedException {
		
		Logger logger = Logger.getLogger(MainCrawlerHandler.class.getName());
		File configFile = new File(DEFAULT_CONFIG_FILE);
		
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(configFile));
			LoggerHelper.log(logger, Level.INFO, "Read " + configFile.getAbsolutePath());
		} catch (Exception e) {
			LoggerHelper.logError(logger, e, "Could not open mavencrawler.conf file.");
			System.exit(1);
		}
				
		MongoDBHandler mongoHandler = MongoDBHandler.newInstance(logger, properties);
		RabbitMQHandler rabbitHandler = RabbitMQHandler.newInstance(logger, properties);
		
		while (true) {
			enqueue(mongoHandler, rabbitHandler, logger);
			// Wait one hour to enqueue again
			Thread.sleep(60*60*1000);
		}
	}

	public static void enqueue(MongoDBHandler mongoHandler, RabbitMQHandler rabbitHandler, Logger logger) {
		
		Gson gson = new Gson();
		
		Consumer<Metadata> metadataConsumer = metadata -> {
			
			for (String version : metadata.getVersions()) {
				Downloaded toDownload = new Downloaded(
						metadata.getGroupId(), 
						metadata.getArtifactId(),
						metadata.getRepository(), 
						version, 
						null,
						null);
				
				if (!Downloaded.exists(toDownload, mongoHandler.getMongoDatabase())) {
					rabbitHandler.sendMessage(gson.toJson(toDownload));
				}

			}
		};
		
		Metadata.iterateAllInMongo(mongoHandler.getMongoDatabase(), metadataConsumer);
	}
	
}
