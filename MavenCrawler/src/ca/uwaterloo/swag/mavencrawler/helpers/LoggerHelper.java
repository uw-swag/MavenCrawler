package ca.uwaterloo.swag.mavencrawler.helpers;

import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggerHelper {

	public static void logError(Logger logger, Exception e, String message) {
		e.printStackTrace();
		String theError = message;
		if (e.getStackTrace().length>=1){
			theError += "\n" + e.getStackTrace()[0].toString();
		}
		LoggerHelper.log(logger, Level.SEVERE, theError);
	}

	public static void log(Logger logger, Level level, String message) {
		if (logger != null) {
			logger.log(level, message);
		}
	}
}
