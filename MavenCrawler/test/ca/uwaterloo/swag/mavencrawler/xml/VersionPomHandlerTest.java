package ca.uwaterloo.swag.mavencrawler.xml;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.uwaterloo.swag.mavencrawler.pojo.VersionPom;

public class VersionPomHandlerTest {

	private SAXParser parser;
	private VersionPomHandler handler;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		parser = SAXParserFactory.newInstance().newSAXParser();
		handler = new VersionPomHandler();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParsingVersionPom() {
		
		// Given
		InputStream stream = this.getClass().getResourceAsStream("../versionpom-example.pom");
		
		// When
		try {
			parser.parse(stream, handler);
		} catch (Exception e) {}
		
		// Then
		VersionPom metadata = handler.getVersionPom();

		assertEquals("log4j-group", metadata.getGroupId());
		assertEquals("log4j-artifact", metadata.getArtifactId());
		assertEquals("Apache Log4j", metadata.getName());
		assertEquals("1.2.16", metadata.getVersion());
		assertEquals("Apache Log4j 1.2", metadata.getDescription());
		assertEquals("http://logging.apache.org/log4j/1.2/", metadata.getProjectUrl());
		assertEquals("scm:svn:http://svn.apache.org/repos/asf/logging/log4j/tags/v1_2_16", metadata.getScmConnection());
		assertEquals("http://svn.apache.org/viewvc/logging/log4j/tags/v1_2_16", metadata.getScmUrl());
	}

}
