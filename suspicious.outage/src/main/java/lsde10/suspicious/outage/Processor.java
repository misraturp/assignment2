package lsde10.suspicious.outage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.sentence.SentenceException;
import dk.dma.ais.sentence.Vdm;
import scala.Tuple2;


public class Processor implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	private static Processor instance = null;

	private Processor() {
	}

	public static Processor getInstance() {
		if (instance == null) {
			instance = new Processor();
		}
		return instance;
	}
	
	public String cleanAISMsg(String line){
		if(!line.startsWith("!")){
			int index = line.indexOf("!", 0);
			return line.substring(index);
		}
		return line;
	}
	
	
	public Tuple2<String, String> cleanAISMsg(Tuple2<String, String> file){
		// TODO maybe \r\n is not the right character sequence for newline, it could be \n
		
		String[] content = file._2().split("\r\n");
		List<String> ret = new ArrayList<String>();
		
		for(int i = 0; i < content.length; i++){
			String line = content[i];
			
			if(!line.startsWith("!")){
				int index = line.indexOf("!", 0);
				if(index == -1){
					continue;
				}
				ret.add(line.substring(index));
			}
			else ret.add(line);
		}
		
		return new Tuple2<String,String>(file._1(), String.join("\r\n", ret));
	}
	
	public AisMessage decodeAisMessage(String msg){
		Vdm vdm = new Vdm();
        try {
			vdm.parse(msg);
			AisMessage message = AisMessage.getInstance(vdm);
			return message;
		} catch (SentenceException | AisMessageException | SixbitException e) {
			return null;
		}
	}
	
	/*
	public List<AisMessage> decodeAISMessage(Tuple2<String, String> file) {
		List<AisMessage> ret = new ArrayList<AISMessage>();
		String content = file._2();
		InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

		/*AISInputStreamReader streamReader
    	= new AISInputStreamReader(
    			stream,
                aisMessage -> ret.add(aisMessage));
		
		AISInputStreamReader streamReader
    	= new AISInputStreamReader(
    			stream, null
                );
		
    	
    	try {
			streamReader.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return ret;
	}
	
	
	
	public AISMessage compareLatitude (AISMessage msg1, AISMessage msg2, boolean max)
	{
		float lat1 = 0, lat2 = 0;
		switch(msg1.getMessageType()){
		case PositionReportClassAScheduled : 
			PositionReportClassAScheduled r1 = (PositionReportClassAScheduled) msg1;
			lat1 = r1.getLatitude();
			break;
		case PositionReportClassAAssignedSchedule : 
			PositionReportClassAAssignedSchedule r2 = (PositionReportClassAAssignedSchedule) msg1;
			lat1 = r2.getLatitude();
			break;
		case PositionReportClassAResponseToInterrogation : 
			PositionReportClassAResponseToInterrogation r3 = (PositionReportClassAResponseToInterrogation) msg1;
			lat1 = r3.getLatitude();
			break;
		default:
			return null;
		}
		
		switch(msg2.getMessageType()){
		case PositionReportClassAScheduled : 
			PositionReportClassAScheduled p1 = (PositionReportClassAScheduled) msg2;
			lat2 = p1.getLatitude();
			break;
		case PositionReportClassAAssignedSchedule : 
			PositionReportClassAAssignedSchedule p2 = (PositionReportClassAAssignedSchedule) msg2;
			lat2 = p2.getLatitude();
			break;
		case PositionReportClassAResponseToInterrogation : 
			PositionReportClassAResponseToInterrogation p3 = (PositionReportClassAResponseToInterrogation) msg2;
			lat2 = p3.getLatitude();
			break;
		default:
			return null;
		}
		
		if(max)
		{
			if(lat1>=lat2)
				return msg1;
			else
				return msg2;
		}
		else
		{
			if(lat1<=lat2)
				return msg1;
			else
				return msg2;
		}
		
	}
	
	public AISMessage compareLongitude (AISMessage msg1, AISMessage msg2, boolean max)
	{
		float lon1 = 0, lon2 = 0;
		switch(msg1.getMessageType()){
		case PositionReportClassAScheduled : 
			PositionReportClassAScheduled r1 = (PositionReportClassAScheduled) msg1;
			lon1 = r1.getLongitude();
			break;
		case PositionReportClassAAssignedSchedule : 
			PositionReportClassAAssignedSchedule r2 = (PositionReportClassAAssignedSchedule) msg1;
			lon1 = r2.getLongitude();
			break;
		case PositionReportClassAResponseToInterrogation : 
			PositionReportClassAResponseToInterrogation r3 = (PositionReportClassAResponseToInterrogation) msg1;
			lon1 = r3.getLongitude();
			break;
		default:
			return null;
		}
		
		switch(msg2.getMessageType()){
		case PositionReportClassAScheduled : 
			PositionReportClassAScheduled p1 = (PositionReportClassAScheduled) msg2;
			lon2 = p1.getLongitude();
			break;
		case PositionReportClassAAssignedSchedule : 
			PositionReportClassAAssignedSchedule p2 = (PositionReportClassAAssignedSchedule) msg2;
			lon2 = p2.getLongitude();
			break;
		case PositionReportClassAResponseToInterrogation : 
			PositionReportClassAResponseToInterrogation p3 = (PositionReportClassAResponseToInterrogation) msg2;
			lon2 = p3.getLongitude();
			break;
		default:
			return null;
		}
		
		if(max)
		{
			if(lon1>=lon2)
				return msg1;
			else
				return msg2;
		}
		else
		{
			if(lon1<=lon2)
				return msg1;
			else
				return msg2;
		}
		
	}
	
	public float getValue(AISMessage msg, boolean lat)
	{
		switch(msg.getMessageType()){
		case PositionReportClassAScheduled : 
			PositionReportClassAScheduled r1 = (PositionReportClassAScheduled) msg;
			if(lat)
				return r1.getLatitude();
			else
				return r1.getLongitude();
		case PositionReportClassAAssignedSchedule : 
			PositionReportClassAAssignedSchedule r2 = (PositionReportClassAAssignedSchedule) msg;
			if(lat)
				return r2.getLatitude();
			else
				return r2.getLongitude();
		case PositionReportClassAResponseToInterrogation : 
			PositionReportClassAResponseToInterrogation r3 = (PositionReportClassAResponseToInterrogation) msg;
			if(lat)
				return r3.getLatitude();
			else
				return r3.getLongitude();
		default:
			return 0;
		}
	}
	
	public void trainGridMap(AisMessage msg){
		
	}*/
	
}
