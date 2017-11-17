package ca.uwaterloo.swag.mavencrawler.xml;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import ca.uwaterloo.swag.mavencrawler.pojo.Archetype;
import ca.uwaterloo.swag.mavencrawler.xml.ArchetypeCatalogHandler;

public class ArchetypeCatalogHandlerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParsing() throws ParserConfigurationException, SAXException, IOException {
		
		// Given
		InputStream stream = this.getClass().getResourceAsStream("../archetype-catalog-example.xml");
		SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
		ArchetypeCatalogHandler handler = new ArchetypeCatalogHandler();
		
		// When
		parser.parse(stream, handler);
		
		// Then
		assertEquals(2, handler.getArchetypes().size());
		
		Archetype archetype = handler.getArchetypes().get(0);
		assertEquals("am.ik.archetype", archetype.getGroupId());
		assertEquals("maven-reactjs-blank-archetype", archetype.getArtifactId());
		assertEquals("1.0.0", archetype.getVersion());
		assertEquals("http://central.maven.org", archetype.getRepository());
		assertEquals("Blank Project for React.js", archetype.getDescription());
		
		archetype = handler.getArchetypes().get(1);
		assertEquals("us.fatehi", archetype.getGroupId());
		assertEquals("schemacrawler-archetype-plugin-lint", archetype.getArtifactId());
		assertEquals("11.02.01", archetype.getVersion());
		assertNull(archetype.getRepository());
		assertNull(archetype.getDescription());
	}

}
