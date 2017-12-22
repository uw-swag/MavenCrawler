package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.FileNotFoundException;
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
import ca.uwaterloo.swag.mavencrawler.pojo.Downloaded;
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
		logger.log(Level.INFO, "Crawling " + mavenURLS + "...");
		crawlMetadataFromMavenRoots(mavenURLS);
		logger.log(Level.INFO, "Downloading libraries...");
		downloadLibraries();
	}

	public void crawlMetadataFromMavenRoots(List<String> mavenRoots) {
		MetadataCrawler.crawlMavenRoots(mavenRoots, mongoHandler.getMongoDatabase(), logger);
	}

	public void crawlCatalogFromMavenRoot(String mavenRootURL) {
		
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

			LoggerHelper.log(logger, Level.INFO, "Parsing archetype-catalog.xml...");
			parser.parse(stream, archetypeHandler);
			LoggerHelper.log(logger, Level.INFO, "Parsed " + archetypeHandler.getArchetypes().size() + " archetypes.");

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

	public void updateArchetypes() {
		List<Archetype> arquetypes = Archetype.findAllFromMongo(mongoHandler.getMongoDatabase());
		
		for (Archetype archetype : arquetypes) {
			updateMetadataForArchetype(archetype);
		}
	}

	public void updateMetadataForArchetype(Archetype archetype) {
		
		try {
			MavenMetadataHandler metadataHandler = new MavenMetadataHandler();
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			
			LoggerHelper.log(logger, Level.INFO, "Parsing maven-metadata.xml...");
			parser.parse(archetype.getMetadataURL().openStream(), metadataHandler);
			LoggerHelper.log(logger, Level.INFO, "Parsed " + metadataHandler.getMetadata() + ".");
			
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

	private void saveDownloaded(Metadata metadata, String version, File downloadFile) {
		Downloaded downloaded = new Downloaded(metadata.getGroupId(), 
											  metadata.getArtifactId(), 
											  metadata.getRepository(), 
											  version, 
											  new Date(), 
											  downloadFile.getAbsolutePath());
		Downloaded.upsertInMongo(downloaded, mongoHandler.getMongoDatabase(), logger);
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

	public void downloadLibraries() {
		List<Metadata> metadataList = Metadata.findAllFromMongo(mongoHandler.getMongoDatabase());
		
		for (Metadata metadata : metadataList) {
			downloadLibrariesFromMetadata(metadata);
		}
	}
	
}
