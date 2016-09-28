package lsde10.suspicious.outage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.eventbus.Subscribe;

import dk.tbsalling.ais.tracker.AISTrack;
import dk.tbsalling.ais.tracker.AISTracker;
import dk.tbsalling.ais.tracker.events.AisTrackCreatedEvent;
import dk.tbsalling.ais.tracker.events.AisTrackDeletedEvent;
import dk.tbsalling.ais.tracker.events.AisTrackDynamicsUpdatedEvent;
import dk.tbsalling.ais.tracker.events.AisTrackUpdatedEvent;
import dk.tbsalling.aismessages.AISInputStreamReader;

public class SuspiciousOutage {

	public static void main( String[] args )
    {
		String path = System.getProperty("user.dir");
		//File data = new File(path + "//data//03//06-00.txt");
		//InputStream inputStream = null;
		
		
		List<InputStream> iss = null;
		try {
			iss = Files.list(Paths.get(path + "//data//03//"))
			        .filter(Files::isRegularFile)
			        .map(f -> {
			            try {
			                return new FileInputStream(f.toString());
			            } catch (Exception e) {
			                throw new RuntimeException(e);
			            }
			        }).collect(Collectors.toList());
		} catch (IOException e1) {
			
			e1.printStackTrace();
		}

		SequenceInputStream stream = new SequenceInputStream(Collections.enumeration(iss));
		
		
		try {
			//inputStream = new FileInputStream(data);
			
//			AISInputStreamReader streamReader
//        	= new AISInputStreamReader(
//        			inputStream,
//                    aisMessage -> System.out.println(aisMessage));
//			streamReader.run();
		
            // Start tracking
            AISTracker tracker = new AISTracker();
            
            
            tracker.registerSubscriber(new Object() {
                @Subscribe
                public void handleEvent(AisTrackCreatedEvent event) {
                    //Nothing todo here
                	//System.out.println("CREATED: " + event.getAisTrack());
                }

                @Subscribe
                public void handleEvent(AisTrackUpdatedEvent event) {
                    AISTrack track = event.getAisTrack();
                    
                    long last = track.getTimeOfLastUpdate().getEpochSecond();
                    long nieuw  = track.getTimeOfDynamicUpdate().getEpochSecond();
                    
//                    if (timeOfStaticUpdate == null)
//                        return timeOfDynamicUpdate;
//                    else if (timeOfDynamicUpdate == null)
//                        return timeOfStaticUpdate;
//                    else
//                        return timeOfStaticUpdate.isBefore(timeOfDynamicUpdate) ? timeOfDynamicUpdate : timeOfStaticUpdate;
                    
                    float diff = (float) ((nieuw - last)/60);
                    if(diff > 0.1){
                    	System.out.println("Last updated: " + track.getTimeOfLastUpdate() );
                    	System.out.println("Difference is :" + diff);
                    }
                    //System.out.println("UPDATED: " + event.getAisTrack());
                }

                @Subscribe
                public void handleEvent(AisTrackDynamicsUpdatedEvent event) {
                    //Nothing to do here
                	//System.out.println("UPDATED DYNAMICS: " + event.getAisTrack());
                }

                @Subscribe
                public void handleEvent(AisTrackDeletedEvent event) {
                    //Nothing todo here
                	//System.out.println("DELETED: " + event.getAisTrack());
                }
            });

            // Feed AIS Data into tracker
            tracker.update(stream);
        
		} catch (IOException e) {
			e.printStackTrace();
		}
		
        Set<AisTrack> tracks = tracker.getAisTracks();
        AISTrack aisIterator = tracks.iterator().next();
		
        Path file = Paths.get("output.txt");
        List<String> lines = Arrays.asList(aisIterator.getMmsi(), aisIterator.getShipName(),aisIterator.getLatitude(),aisIterator.getLongitude());
        Files.write(file, lines , Charset.forName("UTF-8"));
		
    }

}
