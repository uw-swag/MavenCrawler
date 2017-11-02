package ca.uwaterloo.swag.mavencrawler;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import ca.uwaterloo.swag.mavencrawler.db.MongoDBPersister;
import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.xml.ArchetypeCatalogHandler;

public class Crawler {
	
	private Logger logger;
	private MongoDBPersister persister;

	public Crawler(Logger logger, MongoDBPersister persister) {
		super();
		this.logger = logger;
		this.persister = persister;
	}

	public Logger getLogger() {
		return logger;
	}
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	public MongoDBPersister getPersister() {
		return persister;
	}
	public void setPersister(MongoDBPersister persister) {
		this.persister = persister;
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
			crawlMavenArchetypeXMLInputStream(url.openStream());
		} 
		catch (MalformedURLException e) {
			LoggerHelper.logError(logger, e, "Bad URL: " + mavenRootURL);
		} catch (IOException e) {
			LoggerHelper.logError(logger, e, "Could not retrieve " + mavenRootURL + "/archetype-catalog.xml");
		}
	}
	
	protected void crawlMavenArchetypeXMLInputStream(InputStream stream) {

		try {
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			ArchetypeCatalogHandler handler = new ArchetypeCatalogHandler();
			logger.log(Level.INFO, "Parsing archetype-catalog.xml...");
			parser.parse(stream, handler);
			logger.log(Level.INFO, "Parsed " + handler.getArchetypes().size() + " archetypes.");

			persister.upsertArchetypes(handler.getArchetypes());
		} 
		catch (ParserConfigurationException | SAXException | IOException e) {
			LoggerHelper.logError(logger, e, "Error parsing archetype-catalog.xml.");
		}
	}
	
}
