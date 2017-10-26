package ca.uwaterloo.swag.mavencrawler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class MavenMetadataHandlerTest {

	private SAXParser parser;
	private MavenMetadataHandler handler;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		parser = SAXParserFactory.newInstance().newSAXParser();
		handler = new MavenMetadataHandler();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParsingMultipleVersions() throws ParserConfigurationException, SAXException, IOException {
		
		// Given
		InputStream stream = this.getClass().getResourceAsStream("maven-metadata-example-multiple.xml");
		
		// When
		parser.parse(stream, handler);
		
		// Then
		Metadata metadata = handler.getMetadata();
		
		assertEquals("log4j-group", metadata.getGroupId());
		assertEquals("log4j-artifact", metadata.getArtifactId());
		assertEquals("1.2.17-latest", metadata.getLatest());
		assertEquals("1.2.17-release", metadata.getRelease());
		assertEquals(14, metadata.getVersions().size());
		assertEquals("1.1.3", metadata.getVersions().get(0));
		assertEquals("1.2.17", metadata.getVersions().get(13));

		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, 2014);
		cal.set(Calendar.MONTH, Calendar.MARCH);
		cal.set(Calendar.DAY_OF_MONTH, 18);
		cal.set(Calendar.HOUR_OF_DAY, 15);
		cal.set(Calendar.MINUTE, 44);
		cal.set(Calendar.SECOND, 02);
		cal.set(Calendar.MILLISECOND, 0);
		
		assertEquals(cal.getTime(), metadata.getLastUpdated());
	}
	
	@Test
	public void testParsingSingleVersion() throws ParserConfigurationException, SAXException, IOException {
		
		// Given
		InputStream stream = this.getClass().getResourceAsStream("maven-metadata-example-single.xml");
		
		// When
		parser.parse(stream, handler);
		
		// Then
		Metadata metadata = handler.getMetadata();

		assertEquals("log4j-group", metadata.getGroupId());
		assertEquals("log4j-artifact", metadata.getArtifactId());
		assertNull(metadata.getLatest());
		assertNull(metadata.getRelease());
		assertEquals(1, metadata.getVersions().size());
		assertEquals("1.1.3", metadata.getVersions().get(0));
		assertNull(metadata.getLastUpdated());
	}

}
