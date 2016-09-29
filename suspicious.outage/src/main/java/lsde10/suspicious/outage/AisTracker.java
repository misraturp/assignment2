package lsde10.suspicious.outage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AisTracker {
	
	static HashMap<Integer, List<Track>> trackMap = new HashMap<Integer, List<Track>>();
	
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
                try{
	                Track temp = new Track(Float.valueOf(info[1]), 
					                		Float.valueOf(info[2]), 
					                		Integer.valueOf(info[3]));
	                if(trackMap.containsKey(temp_mmsi))
	                	trackMap.get(temp_mmsi).add(temp);
	                else{
	                	List<Track> l = new ArrayList<Track>();
	                	l.add(temp);
	                	trackMap.put(temp_mmsi, l);
	                }
	                	
                }
                catch (ArrayIndexOutOfBoundsException e){
                	
                }
                
            	
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
	
	public static List<Track> getTrack(int req_mmsi){
		return trackMap.get(req_mmsi);		
	}
	
	public static List<Integer> findOutage(Track track){
		return null;
	}
	
}
