package lsde10.suspicious.outage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class PreProcessClean {

	public static void main(String[] args) {
		String path = System.getProperty("user.dir");

		File dir = new File(path + "//data//03//");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
		    for (File child : directoryListing) {
		    	
		    	
				try {
					BufferedReader reader = new BufferedReader(new FileReader(child));
					String fileP = path + "//data//03//" + "_" + child.getName();
			    	BufferedWriter writer = new BufferedWriter(new FileWriter(fileP));
			    	
			    	String line = reader.readLine();
					while(line != null){
						if(!line.startsWith("!")){
							int index = line.indexOf("!", 0);
							if(index == -1){
								//System.out.println(line + "\n");
								line = reader.readLine();
								continue;
							}
							writer.write(line.substring(index));
						}
						else writer.write(line);
						
						writer.newLine();
						line = reader.readLine();	
			    	}
					
					reader.close();
					writer.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		    	
		    }
		}	
	}
}
