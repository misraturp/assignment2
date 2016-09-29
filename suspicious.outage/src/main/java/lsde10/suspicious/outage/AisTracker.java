package lsde10.suspicious.outage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class AisTracker {
	
	static HashMap<Integer, List<Track>> trackMap = new HashMap<Integer, List<Track>>();
	
	static Set<Integer> shipsSeen = new HashSet<Integer>();
	
	public AisTracker()
	{}

	public static void readCsv(String filename)
	{
		BufferedReader buffer = null;
        String line = "";
        String cvsSplitBy = ",";
        int temp_mmsi = 0;
        SimpleDateFormat sdf = new java.text.SimpleDateFormat ("HHmm");
        
        try {

            buffer = new BufferedReader(new FileReader(filename));
            while ((line = buffer.readLine()) != null) {

                String[] info = line.split(cvsSplitBy);
                temp_mmsi = Integer.valueOf(info[0]);
                try{
	                Track temp = new Track(Float.valueOf(info[1]), 
					                		Float.valueOf(info[2]), 
					                		sdf.parse(info[3]));
	                if(trackMap.containsKey(temp_mmsi))
	                	trackMap.get(temp_mmsi).add(temp);
	                else{
	                	List<Track> l = new ArrayList<Track>();
	                	l.add(temp);
	                	trackMap.put(temp_mmsi, l);
	                }
	                shipsSeen.add(temp_mmsi);
	                	
                }
                catch (ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e){
                	
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
	
	public static List<Long> findOutage(List<Track> tracks, int minDiff){
		Date last = tracks.get(0).getTime();
		long diff = 0;
		List<Long> diffs = new LinkedList<Long>();
		for(Track track : tracks){
			diff = track.getTime().getTime() - last.getTime();
	        diff = diff / (60 * 1000);
	        
			last = track.getTime();
			
			if(diff > minDiff){
				diffs.add(diff);
			}
		}
		diffs.sort((a,b) ->  b.compareTo(a));
		
		return diffs;
	}
	
	public static void printOutages(int minDiff){
		for(Integer i : shipsSeen){
			
			List<Long> l = findOutage(getTrack(i), minDiff);
			if(!l.isEmpty())
				System.out.println("MMSI: " + i.toString() + " " 
						+ l.toString() );
		}
	}
	
}
