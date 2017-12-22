package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import com.mongodb.client.MongoDatabase;

import ca.uwaterloo.swag.mavencrawler.helpers.LoggerHelper;
import ca.uwaterloo.swag.mavencrawler.pojo.Metadata;
import ca.uwaterloo.swag.mavencrawler.xml.MavenMetadataHandler;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import edu.uci.ics.crawler4j.url.WebURL;

public class MetadataCrawler extends WebCrawler {

    private final static Pattern FILTERS = Pattern.compile(".*(\\.(md5|sha1|asc|jar|aar))$");

	private Logger logger;
	private MongoDatabase mongoDatabase;
	
	public MetadataCrawler(Logger logger, MongoDatabase mongoDatabase) {
		super();
		this.logger = logger;
		this.mongoDatabase = mongoDatabase;
	}
	
	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public MongoDatabase getMongoDatabase() {
		return mongoDatabase;
	}

	public void setMongoDatabase(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
	}
	
	@Override
	protected WebURL handleUrlBeforeProcess(WebURL curURL) {
		
		WebURL handledURL = new WebURL();
		handledURL.setURL(curURL.getURL().replaceAll("/:", "/"));
		
		return handledURL;
	}
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		
		// Only crawl down folders
		if (referringPage != null && !url.getURL().startsWith(referringPage.getWebURL().getURL())) {
			return false;
		}
		
		// Do not visit catalog
		if (url.getURL().toLowerCase().endsWith("archetype-catalog.xml")) {
			return false;
		}
		
		// Avoid visiting unwanted files
		return !FILTERS.matcher(url.getURL().toLowerCase()).matches();
	}

	@Override
	public void visit(Page page) {
		
		if (page.getWebURL().getURL().endsWith("maven-metadata.xml")) {
			
			try {
				URL url = new URL(page.getWebURL().getURL());
				MavenMetadataHandler metadataHandler = new MavenMetadataHandler();
				SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
				
				LoggerHelper.log(logger, Level.INFO, "Parsing maven-metadata.xml...");
				parser.parse(url.openStream(), metadataHandler);
				LoggerHelper.log(logger, Level.INFO, "Parsed " + metadataHandler.getMetadata() + ".");
				
				metadataHandler.getMetadata().setRepository(url.getProtocol() + "://" + url.getHost());
				Metadata.upsertInMongo(metadataHandler.getMetadata(), mongoDatabase, logger);
				
			} catch (MalformedURLException e) {
				LoggerHelper.logError(logger, e, "Bad URL: " + page.getWebURL());
			} catch (ParserConfigurationException | SAXException | IOException e) {
				LoggerHelper.logError(logger, e, "Error parsing maven-metadata.xml.");
			}
		}
		
	}

	public static void crawlMavenRoots(List<String> mavenRoots, MongoDatabase mongoDatabase, Logger logger) {

		String tempCrawlStorageFolder = new File("crawlerTemp").getAbsolutePath();
		int numberOfCrawlers = Runtime.getRuntime().availableProcessors();

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(tempCrawlStorageFolder);
		config.setIncludeBinaryContentInCrawling(true);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller;
		try {
			controller = new CrawlController(config, pageFetcher, robotstxtServer);
		} catch (Exception e) {
			LoggerHelper.log(logger, Level.SEVERE, "Could not create temp folder for MetadataCrawler");
			return;
		}

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */
        for (String mavenRoot : mavenRoots) {
            controller.addSeed(mavenRoot);
        }
        
		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		MetadataCrawlerFactory metadataCrawlerFactory = new MetadataCrawlerFactory(logger, mongoDatabase);
		controller.start(metadataCrawlerFactory, numberOfCrawlers);
	}

}
