package ca.uwaterloo.swag.mavencrawler.helpers;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class StringHelper {
	
	public static List<String> getStringsFromFile(String filePath) {
		
		ArrayList<String> list = new ArrayList<String>();
		
		try {
			Scanner scanner = new Scanner(new File(filePath));
			
			while (scanner.hasNext()){
			    list.add(scanner.next());
			}
			scanner.close();
		} 
		catch (FileNotFoundException e) {
			// Do nothing, return an empty list
		}
		
		return list;
	}

}
