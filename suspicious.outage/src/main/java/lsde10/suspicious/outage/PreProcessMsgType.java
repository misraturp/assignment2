package lsde10.suspicious.outage;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import dk.tbsalling.aismessages.AISInputStreamReader;
import dk.tbsalling.aismessages.ais.messages.AISMessage;
import dk.tbsalling.aismessages.ais.messages.types.AISMessageType;

public class PreProcessMsgType {
	static long nonPos = 0;
	static long Pos = 0;
	//boolean init = false;
	
	private static void updatePositionInformationRatio(AISMessage msg){
		if( (msg.getMessageType() == AISMessageType.PositionReportClassAScheduled )
			|| (msg.getMessageType() == AISMessageType.PositionReportClassAAssignedSchedule )
			|| (msg.getMessageType() == AISMessageType.PositionReportClassAResponseToInterrogation))
			Pos++;
		nonPos++;
		//System.out.println(msg.getMessageType());
	}

	public static void main(String[] args) {
		nonPos = 0;
		Pos = 0;
		
		String path = System.getProperty("user.dir");

		List<FileInputStream> iss = null;
		try {
			iss = Files.list(Paths.get(path + "//data//03//"))
			        .filter(Files::isRegularFile)
			        .filter(file -> file.getFileName().toString().startsWith("_")) 
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
//		for(FileInputStream fis : iss){
//			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
//			//BufferedWriter writer = new BufferedWriter(new FileWriter(fis.) )
//	          
//            System.out.println("Reading File line by line using BufferedReader");
//          
//            String line;
//			try {
//				line = reader.readLine();
//				while(line != null){
//					
//	                System.out.println(line);
//	                line = reader.readLine();
//	                
//	            }           
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//            
//		}
		SequenceInputStream stream = new SequenceInputStream(Collections.enumeration(iss));

		
		AISInputStreamReader streamReader
    	= new AISInputStreamReader(
    			stream,
                aisMessage -> updatePositionInformationRatio( aisMessage));
		
		
		 
		try {
			streamReader.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println((double) (Pos/nonPos));
	}
	
}
