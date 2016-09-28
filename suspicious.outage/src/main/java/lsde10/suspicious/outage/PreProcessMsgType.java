package lsde10.suspicious.outage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
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
import dk.tbsalling.aismessages.ais.messages.PositionReportClassAAssignedSchedule;
import dk.tbsalling.aismessages.ais.messages.PositionReportClassAResponseToInterrogation;
import dk.tbsalling.aismessages.ais.messages.PositionReportClassAScheduled;
import dk.tbsalling.aismessages.ais.messages.types.AISMessageType;

public class PreProcessMsgType {
	static long nonPos = 0;
	static long Pos = 0;
	static BufferedWriter writer = null;
	static String currentSecond = null;
	//boolean init = false;
	
	private static void processMessage(AISMessage msg){
		if(msg.getSourceMmsi().getMMSI() > 0){
			try {
			switch(msg.getMessageType()){
				case PositionReportClassAScheduled : 
					PositionReportClassAScheduled r1 = (PositionReportClassAScheduled) msg;
				
					writer.write(r1.getSourceMmsi().getMMSI().toString() + ','
							+ r1.getLatitude().toString() + ','
							+ r1.getLongitude().toString() + ','
							+ currentSecond +'\n');
				
					break;
				case PositionReportClassAAssignedSchedule : 
					PositionReportClassAAssignedSchedule r2 = (PositionReportClassAAssignedSchedule) msg;
					writer.write(r2.getSourceMmsi().getMMSI().toString() + ','
							+ r2.getLatitude().toString() + ','
							+ r2.getLongitude().toString() + ','
							+ currentSecond +'\n');
					break;
				case PositionReportClassAResponseToInterrogation : 
					PositionReportClassAResponseToInterrogation r3 = (PositionReportClassAResponseToInterrogation) msg;
					writer.write(r3.getSourceMmsi().getMMSI().toString() + ','
							+ r3.getLatitude().toString() + ','
							+ r3.getLongitude().toString() + ','
							+ currentSecond +'\n');
					break;
			}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//writer.write(msg.getSourceMmsi().getMMSI() + ',' + msg.dataFields().get(key));
			
		//System.out.println(msg.getMessageType());
	}

	public static void main(String[] args) {
		nonPos = 0;
		Pos = 0;
		
		String path = System.getProperty("user.dir");
		
		File dir = new File(path + "//data//03//");
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
		    for (File child : directoryListing) {
		    	if(child.getName().startsWith("_")){
		    		try {
						FileInputStream is = new FileInputStream(child);
						String fileP = path + "//data//03//" + "!" + child.getName().substring(1);
				    	currentSecond = child.getName().substring(1,3) + child.getName().substring(4,6);
						System.out.println(currentSecond);
						writer = new BufferedWriter(new FileWriter(fileP));
				    	
				    	AISInputStreamReader streamReader
				    	= new AISInputStreamReader(
				    			is,
				                aisMessage -> processMessage(aisMessage));
				    	
				    	streamReader.run();
		    		
		    		}
		    		catch(IOException e){
		    			
		    		}
		    	}
		    	
		    }
		}

//		List<FileInputStream> iss = null;
//		try {
//			iss = Files.list(Paths.get(path + "//data//03//"))
//			        .filter(Files::isRegularFile)
//			        .filter(file -> file.getFileName().toString().startsWith("_")) 
//			        .map(f -> {
//			            try {
//			                return new FileInputStream(f.toString());
//			            } catch (Exception e) {
//			                throw new RuntimeException(e);
//			            }
//			        }).collect(Collectors.toList());
//		} catch (IOException e1) {
//			
//			e1.printStackTrace();
//		}
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
//		SequenceInputStream stream = new SequenceInputStream(Collections.enumeration(iss));

		
//		AISInputStreamReader streamReader
//    	= new AISInputStreamReader(
//    			stream,
//                aisMessage -> processMessage(aisMessage));
//		
		
		 
//		try {
//			streamReader.run();
//		} catch (IOException e) {
//			
//			e.printStackTrace();
//		}	
	}
	
}
