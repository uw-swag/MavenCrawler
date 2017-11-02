package ca.uwaterloo.swag.mavencrawler.helpers;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringHelperTest {

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
	public void testGetStringsFromFile() {
		
		// Given
		String filePath = this.getClass().getResource("../mavenURLs-example.list").getPath();
		
		// When
		List<String> urls = StringHelper.getStringsFromFile(filePath);
		
		// Then
		assertEquals(2, urls.size());
		assertEquals("http://central.maven.org/maven2", urls.get(0));
		assertEquals("https://jcenter.bintray.com", urls.get(1));
	}

}
