package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBHandler;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.pojo.Archetype;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;
import ca.uwaterloo.swag.mavencrawler.pojo.Repository;
import ca.uwaterloo.swag.mavencrawler.xml.ArchetypeCatalogHandler;
import ca.uwaterloo.swag.mavencrawler.xml.MavenMetadataHandler;

public class Crawler {
	
	private Logger logger;
	private MongoDBHandler mongoHandler;
	private String downloadFolder;

	public Crawler(Logger logger, MongoDBHandler handler, String downloadFolder) {
		super();
		this.logger = logger;
		this.mongoHandler = handler;
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

	public void crawlMavenURLs(List<String> mavenURLS) {
		for (String url : mavenURLS) {
			logger.log(Level.INFO, "Crawling " + url + "...");
			crawlMavenRoot(url);
		}
		updateArchetypes();
	}

	public void crawlMavenRoot(String mavenRootURL) {
		
		try {
			URL url = new URL(mavenRootURL + "/archetype-catalog.xml");
			crawlMavenArchetypeXMLInputStream(url.openStream(), mavenRootURL);
		} 
		catch (MalformedURLException e) {
			LoggerHelper.logError(logger, e, "Bad URL: " + mavenRootURL);
		} catch (IOException e) {
			LoggerHelper.logError(logger, e, "Could not retrieve " + mavenRootURL + "/archetype-catalog.xml");
		}
	}
	
	protected void crawlMavenArchetypeXMLInputStream(InputStream stream, String repositoryURL) {

		try {
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			ArchetypeCatalogHandler archetypeHandler = new ArchetypeCatalogHandler();
			logger.log(Level.INFO, "Parsing archetype-catalog.xml...");
			parser.parse(stream, archetypeHandler);
			logger.log(Level.INFO, "Parsed " + archetypeHandler.getArchetypes().size() + " archetypes.");

			// Set repository URL to default, if null
			archetypeHandler.getArchetypes().forEach(a -> {
				if(a.getRepository() == null) 
					a.setRepository(repositoryURL);
				});
			
			Archetype.upsertInMongo(archetypeHandler.getArchetypes(), mongoHandler.getMongoDatabase(), logger);
			Repository.setLastCheckedDateForURLInMongo(repositoryURL, mongoHandler.getMongoDatabase(), new Date());
		} 
		catch (ParserConfigurationException | SAXException | IOException e) {
			LoggerHelper.logError(logger, e, "Error parsing archetype-catalog.xml.");
		}
	}

	public void updateMetadataForArchetype(Archetype archetype) {
		
		try {
			MavenMetadataHandler metadataHandler = new MavenMetadataHandler();
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			
			logger.log(Level.INFO, "Parsing archetype-catalog.xml...");
			parser.parse(archetype.getMetadataURL().openStream(), metadataHandler);
			logger.log(Level.INFO, "Parsed " + metadataHandler.getMetadata() + ".");
			
			metadataHandler.getMetadata().setRepository(archetype.getRepository());
			Metadata.upsertInMongo(metadataHandler.getMetadata(), mongoHandler.getMongoDatabase(), logger);
			
		} catch (MalformedURLException e) {
			LoggerHelper.logError(logger, e, 
					"Bad URL: " + archetype.getRepository() + "/" + 
					 archetype.getGroupId() + "/" + 
					 archetype.getArtifactId() + "/maven-metadata.xml");
		} catch (ParserConfigurationException | SAXException | IOException e) {
			LoggerHelper.logError(logger, e, "Error parsing maven-metadata.xml.");
		}
	}

	public void updateArchetypes() {
		List<Archetype> arquetypes = Archetype.findAllFromMongo(mongoHandler.getMongoDatabase());
		
		for (Archetype archetype : arquetypes) {
			updateMetadataForArchetype(archetype);
		}
	}

	public void downloadLibrariesFromMetadata(Metadata metadata) {
		
		File downloadFolder = new File(this.getDownloadFolder());
		
		// Check download folder
		if ((!downloadFolder.exists() && !downloadFolder.mkdirs()) ||
			(downloadFolder.exists() && !downloadFolder.isDirectory())) {
			LoggerHelper.log(logger, Level.SEVERE, "Error with download folder");
			return;
		}
		
		for (URL url : metadata.getLibrariesURLs()) {
			
			File downloadFile = new File(downloadFolder, getLibraryFileNameGroupIdAndURL(metadata.getGroupId(), url));
			
			if (!downloadFile.exists()) {
				try {
			        ReadableByteChannel rbc = Channels.newChannel(url.openStream());
					FileOutputStream fos = new FileOutputStream(downloadFile);
			        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			        fos.close();
			        rbc.close();
				} catch (IOException e) {
					LoggerHelper.log(logger, Level.SEVERE, "Error downloading " + downloadFile.getName());
				}
			}
		}
	}

	public void downloadLibraries() {
		List<Metadata> metadataList = Metadata.findAllFromMongo(mongoHandler.getMongoDatabase());
		
		for (Metadata metadata : metadataList) {
			downloadLibrariesFromMetadata(metadata);
		}
	}
	
	// Helpers
	
	private String getLibraryFileNameGroupIdAndURL(String groupId, URL url) {
		String[] components = url.getPath().split("/");
		return groupId + "." + components[components.length-1];
	}

	
}
