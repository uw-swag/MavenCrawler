package ca.uwaterloo.swag.mavencrawler.helpers;

import static org.junit.Assert.*;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class LoggerHelperTest {

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
	public void testNullLoggerShouldBeIgnored() {
		
		// Given
		Logger logger = null;
		
		try {
			// When
			LoggerHelper.log(logger, Level.INFO, "message");
		} catch (Exception e) {
			// Then
			fail("Should not raise exception");
		}
	}

}
