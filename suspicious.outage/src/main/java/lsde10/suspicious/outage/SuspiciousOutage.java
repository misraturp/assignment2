package lsde10.suspicious.outage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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

		
		File dir = new File(path + "//data//03//");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
		    	if(child.getName().startsWith("!")){
		    		AisTracker.readCsv(child.getAbsolutePath());
		    	}
			}
		}
		System.out.println(AisTracker.getTrack(244250734));
		
    }

}
