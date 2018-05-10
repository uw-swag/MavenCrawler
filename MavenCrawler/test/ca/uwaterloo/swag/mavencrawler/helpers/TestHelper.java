package ca.uwaterloo.swag.mavencrawler.helpers;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

public class TestHelper {

	public static boolean deleteRecursive(File file) {
		
		// Delete children inside folder
		if (file.exists() && file.isDirectory()) {
			Arrays.stream(file.listFiles()).forEach(f -> assertTrue(deleteRecursive(f)));
		}
		
		// Delete empty folder and/or file
		if (file.exists()) {
			return file.delete();
		}
		
		return true;
	}

}
