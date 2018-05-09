package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.pojo.Downloaded;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;

public class MavenDownloader {

	private static final String DEFAULT_CONFIG_FILE = "mavencrawler.conf"; 
	private static final String DOWNLOAD_FOLDER_PROPERTY = "DOWNLOAD_FOLDER";

	private Logger logger;
	private MongoDBHandler mongoHandler;
	private String downloadFolder;

	public MavenDownloader(Logger logger, MongoDBHandler mongoHandler, String downloadFolder) {
		super();
		this.logger = logger;
		this.mongoHandler = mongoHandler;
		this.downloadFolder = downloadFolder;
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
	public String getDownloadFolder() {
		return downloadFolder;
	}
	public void setDownloadFolder(String downloadFolder) {
		this.downloadFolder = downloadFolder;
	}

	public void downloadLibraries() {
		List<Metadata> metadataList = Metadata.findAllFromMongo(mongoHandler.getMongoDatabase());
		
		for (Metadata metadata : metadataList) {
			downloadLibrariesFromMetadata(metadata);
		}
	}

	public void downloadLibrariesFromMetadata(Metadata metadata) {
		
		File libDownloadFolder = new File(this.getDownloadFolder(), metadata.getGroupId() + "." + metadata.getArtifactId());
		
		// Check download folder
		if ((!libDownloadFolder.exists() && !libDownloadFolder.mkdirs()) ||
			(libDownloadFolder.exists() && !libDownloadFolder.isDirectory())) {
			LoggerHelper.log(logger, Level.SEVERE, "Error with download folder");
			return;
		}
		
		for (String version : metadata.getVersions()) {
			URL url = metadata.findURLForVersion(version);
			File downloadFile = new File(libDownloadFolder, metadata.buildJARFileNameForVersion(version));
			
			boolean success = false;
			
			try {
				LoggerHelper.log(logger, Level.INFO, "Downloading " + url);
				success = downloadLibFromURLToFile(url, downloadFile);
			} catch (FileNotFoundException e) {
				
				// JAR not found, try AAR
				try {
					URL newURL = new URL(url, url.getPath().substring(0, url.getPath().length()-3) + "aar");
					downloadFile = new File(libDownloadFolder, metadata.buildAARFileNameForVersion(version));

					LoggerHelper.log(logger, Level.INFO, "JAR not found, trying " + newURL);
					success = downloadLibFromURLToFile(newURL, downloadFile);
				} catch (MalformedURLException |FileNotFoundException e1) {
					LoggerHelper.log(logger, Level.SEVERE, "Error downloading " + url);
				}
			}
			
			if (success) {
				saveDownloaded(metadata, version, downloadFile);
			}
			
		}
	}

	private boolean downloadLibFromURLToFile(URL url, File downloadFile) throws FileNotFoundException {
		boolean success = false;
		
		if (downloadFile.exists()) {
			success = true;
		}
		else {
			try {
				ReadableByteChannel rbc = Channels.newChannel(url.openStream());
				FileOutputStream fos = new FileOutputStream(downloadFile);
		        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		        fos.close();
		        rbc.close();
		        success = true;
			} 
			catch (FileNotFoundException e) {
				throw e;
			}
			catch (IOException e) {
				LoggerHelper.log(logger, Level.SEVERE, "Error downloading " + downloadFile.getName());
			}
			
		}
		
		return success;
	}

	private void saveDownloaded(Metadata metadata, String version, File downloadFile) {
		Downloaded downloaded = new Downloaded(metadata.getGroupId(), 
											  metadata.getArtifactId(), 
											  metadata.getRepository(), 
											  version, 
											  new Date(), 
											  downloadFile.getAbsolutePath());
		Downloaded.upsertInMongo(downloaded, mongoHandler.getMongoDatabase(), logger);
	}

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
		
		MavenDownloader downloader = new MavenDownloader(logger, persister, properties.getProperty(DOWNLOAD_FOLDER_PROPERTY));
		
		while (true) {
			// Keep downloading indefinitely
			downloader.downloadLibraries();
			
			// Wait 5 minutes while more libraries metadata are crawled
			try {
				Thread.sleep(5*60*1000);
			} catch (InterruptedException e) {
				LoggerHelper.log(logger, Level.INFO, "Error with thread interruption while waiting for downloads.");
			}
		}
	}

}
