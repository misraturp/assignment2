package lsde10.suspicious.outage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class AisTracker {
	
	static HashMap<Integer, Track> trackMap = new HashMap<Integer, Track>();
	
	public AisTracker()
	{}

	public static void readCsv(String filename)
	{
		BufferedReader buffer = null;
        String line = "";
        String cvsSplitBy = ",";
        int temp_mmsi = 0;
        
        try {

            buffer = new BufferedReader(new FileReader(filename));
            while ((line = buffer.readLine()) != null) {

                String[] info = line.split(cvsSplitBy);
                temp_mmsi = Integer.valueOf(info[0]);
                Track temp = new Track(Float.valueOf(info[1]), 
				                		Float.valueOf(info[2]), 
				                		Integer.valueOf(info[3]));
                trackMap.put(temp_mmsi, temp);
            	
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (buffer != null) {
                try {
                    buffer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
	}
	
	public static Track getTrack(int req_mmsi)
	{
		return trackMap.get(req_mmsi);		
	}
	
}
