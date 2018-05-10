package ca.uwaterloo.swag.mavencrawler;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
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
import ca.uwaterloo.swag.mavencrawler.pojo.VersionPom;
import ca.uwaterloo.swag.mavencrawler.xml.MavenMetadataHandler;
import ca.uwaterloo.swag.mavencrawler.xml.VersionPomHandler;
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
	private List<String> seedURLs;
	
	public MetadataCrawler(Logger logger, MongoDatabase mongoDatabase, List<String> seedURLs) {
		super();
		this.logger = logger;
		this.mongoDatabase = mongoDatabase;
		this.seedURLs = seedURLs;
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
	
	public List<String> getSeedURLs() {
		return seedURLs;
	}

	public void setSeedURL(List<String> seedURLs) {
		this.seedURLs = seedURLs;
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
		
		String pageUrl = page.getWebURL().getURL() != null ? page.getWebURL().getURL() : "";
		
		if (pageUrl.endsWith("maven-metadata.xml")) {
			handleMetadata(pageUrl);
		}
		else if (pageUrl.endsWith(".pom")) {
			handlePom(pageUrl);
		}
		
	}

	private void handleMetadata(String pageUrl) {
		MavenMetadataHandler metadataHandler = new MavenMetadataHandler();
		
		try {
			URL url = new URL(pageUrl);
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			
			LoggerHelper.log(logger, Level.INFO, "Parsing METADATA " + pageUrl);
			parser.parse(url.openStream(), metadataHandler);
			LoggerHelper.log(logger, Level.INFO, "Parsed " + metadataHandler.getMetadata());
			
		} catch (MalformedURLException e) {
			LoggerHelper.logError(logger, e, "Bad URL: " + pageUrl);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			LoggerHelper.logError(logger, e, "Error parsing maven-metadata.xml.");
		}
		finally {
			metadataHandler.getMetadata().setRepository(getSeedURL(pageUrl));
			Metadata.upsertInMongo(metadataHandler.getMetadata(), mongoDatabase, logger);
		}
	}

	private void handlePom(String pageUrl) {
		VersionPomHandler versionPomHandler = new VersionPomHandler();
		
		try {
			URL url = new URL(pageUrl);
			SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
			
			String pomName = pageUrl.substring(pageUrl.lastIndexOf("/"));
			LoggerHelper.log(logger, Level.INFO, "Parsing POM " + pomName);
			parser.parse(url.openStream(), versionPomHandler);
			LoggerHelper.log(logger, Level.INFO, "Parsed " + versionPomHandler.getVersionPom());
			
		} catch (MalformedURLException e) {
			LoggerHelper.logError(logger, e, "Bad URL: " + pageUrl);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			LoggerHelper.logError(logger, e, "Error parsing maven-metadata.xml.");
		}
		finally {
			versionPomHandler.getVersionPom().setRepository(getSeedURL(pageUrl));
			VersionPom.upsertInMongo(Arrays.asList(versionPomHandler.getVersionPom()), mongoDatabase, logger);
		}
	}
	
	private String getSeedURL(String pageURL) {
		
		// Check all seeds as multiple parallel crawlers might end up crawling
		// pages from different seeds
		for (String seedURL : seedURLs) {
			if (pageURL.startsWith(seedURL)) {
				return seedURL;
			}
		}
		
		return "";
	}

	public static void crawlMavenRoots(List<String> mavenRoots, MongoDatabase mongoDatabase, Logger logger) {

		String tempCrawlStorageFolder = new File("crawlerTemp").getAbsolutePath();
		int numberOfCrawlers = Runtime.getRuntime().availableProcessors();

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(tempCrawlStorageFolder);
		config.setIncludeBinaryContentInCrawling(true);
		config.setResumableCrawling(true);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setEnabled(false);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		List<CrawlController> controllers = new ArrayList<>(mavenRoots.size());
		try {
			
			// Create one controller for each Maven root
			for (int i = 0; i < mavenRoots.size(); i++) {
				controllers.add(new CrawlController(config, pageFetcher, robotstxtServer));
			}
			
		} catch (Exception e) {
			LoggerHelper.log(logger, Level.SEVERE, "Could not create temp folder for MetadataCrawler");
			return;
		}

		for (int i = 0; i < controllers.size(); i++) {
			CrawlController controller = controllers.get(i);
			String seedURL = mavenRoots.get(i);
    		MetadataCrawlerFactory metadataCrawlerFactory = new MetadataCrawlerFactory(logger, mongoDatabase, mavenRoots);

			/*
			 * For each crawl, you need to add some seed urls. These are the first
			 * URLs that are fetched and then the crawler starts following links
			 * which are found in these pages
			 */
			controller.addSeed(seedURL);

    		// Start the crawl asynchronously.
    		controller.startNonBlocking(metadataCrawlerFactory, numberOfCrawlers);
		}
		
		// Wait until all controllers are done
		for (CrawlController controller : controllers) {
			controller.waitUntilFinish();
		}
		
	}

}
