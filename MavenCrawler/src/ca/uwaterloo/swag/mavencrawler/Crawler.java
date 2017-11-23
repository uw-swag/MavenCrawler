package ca.uwaterloo.swag.mavencrawler;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
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
import ca.uwaterloo.swag.mavencrawler.pojo.Repository;
import ca.uwaterloo.swag.mavencrawler.xml.ArchetypeCatalogHandler;

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
	public MongoDBHandler getPersister() {
		return mongoHandler;
	}
	public void setPersister(MongoDBHandler persister) {
		this.mongoHandler = persister;
	}
	
	public void crawlMavenURLs(List<String> mavenURLS) {
		for (String url : mavenURLS) {
			logger.log(Level.INFO, "Crawling " + url + "...");
			crawlMavenRoot(url);
		}
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
	
}
