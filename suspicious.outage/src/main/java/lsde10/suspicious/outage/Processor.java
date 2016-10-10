package lsde10.suspicious.outage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisMessage1;
import dk.dma.ais.message.AisMessage2;
import dk.dma.ais.message.AisMessage3;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.message.AisPositionMessage;
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
	
	/*public String cleanAISMsg(String line){
		if(!line.startsWith("!")){
			int index = line.indexOf("!", 0);
			return line.substring(index);
		}
		return line;
	}*/
	
	public String cleanAISMsg(String file){
		// TODO maybe \r\n is not the right character sequence for newline, it could be \n
		
		String[] content = file.split("\r\n");
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
		String str = "";
		for (String s : ret){
			str += ret + "\r\n";
		}
		int ind = str.lastIndexOf("\r\n");
		
		
		return str.substring(0, ind);
	
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
				AisPositionMessage m;
				ret.add(line.substring(index));
			}
			else ret.add(line);
		}
		
		String str = "";
		for (String s : ret){
			str += ret + "\r\n";
		}
		int ind = str.lastIndexOf("\r\n");
		
		return new Tuple2<String,String>(file._1(), str.substring(0, ind));
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
	
	/*public Tuple2<String, List<AisMessage>> decodeAisMessage(Tuple2<String, String> msg){
		String[] content = msg._2().split("\r\n");
		List<AisMessage> ret = new ArrayList<AisMessage>();
		
		
		Vdm vdm = new Vdm();
        
		for(int i = 0; i < content.length; i++){
			String line = content[i];
			try {
				vdm.parse(line);
				AisMessage message = AisMessage.getInstance(vdm);
				if(message instanceof AisPositionMessage)
					ret.add(message);
			} catch (SentenceException | AisMessageException | SixbitException e) {
			}
		}
		
		return new Tuple2<String, List<AisMessage>>(msg._1(), ret);
	}*/
	
	public List<Tuple2<String, AisMessage>> decodeAisMessage(Tuple2<String, String> msg){
		String[] content = msg._2().split("\r\n");
		List<Tuple2<String, AisMessage>> ret = new ArrayList<Tuple2<String, AisMessage>>();
		String filename = msg._1();
		
		Vdm vdm = new Vdm();
        
		for(int i = 0; i < content.length; i++){
			String line = content[i];
			try {
				vdm.parse(line);
				AisMessage message = AisMessage.getInstance(vdm);
				if(message instanceof AisPositionMessage)
					ret.add(new Tuple2<String, AisMessage>(filename, message));
			} catch (SentenceException | AisMessageException | SixbitException e) {
			}
		}
		
		return ret;
	}
	
	
	
	
	public AisMessage maximumLatitude (AisMessage msg1, AisMessage msg2)
	{
		float lat1 = 0, lat2 = 0;
		
		AisPositionMessage r1 = (AisPositionMessage) msg1;
		lat1 = r1.getPos().getLatitude();
		
		AisPositionMessage r2 = (AisPositionMessage) msg2;
		lat2 = r2.getPos().getLatitude();
		
			if(lat1>=lat2)
				return msg1;
			else
				return msg2;
	}
	
	public AisMessage minimumLatitude (AisMessage msg1, AisMessage msg2)
	{
		float lat1 = 0, lat2 = 0;
		AisPositionMessage r1 = (AisPositionMessage) msg1;
		lat1 = r1.getPos().getLatitude();
		
		AisPositionMessage r2 = (AisPositionMessage) msg2;
		lat2 = r2.getPos().getLatitude();

			if(lat1>=lat2)
				return msg2;
			else
				return msg1;
	}
	
	public AisMessage maximumLongtitude (AisMessage msg1, AisMessage msg2)
	{
		float lon1 = 0,lon2 = 0;
		AisPositionMessage r1 = (AisPositionMessage) msg1;
		lon1 = r1.getPos().getLongitude();
		
		AisPositionMessage r2 = (AisPositionMessage) msg2;
		lon2 = r2.getPos().getLongitude();
		
			if(lon1>=lon2)
				return msg1;
			else
				return msg2;
	}
	
	public AisMessage minimumLongtitude (AisMessage msg1, AisMessage msg2)
	{
		float lon1 = 0,lon2 = 0;
		AisPositionMessage r1 = (AisPositionMessage) msg1;
		lon1 = r1.getPos().getLongitude();
		
		AisPositionMessage r2 = (AisPositionMessage) msg2;
		lon2 = r2.getPos().getLongitude();
		
			if(lon1>=lon2)
				return msg2;
			else
				return msg1;
	}
	
	public float getValue(AisMessage msg, boolean lat)
	{
		AisMessage1 r1 = (AisMessage1) msg;
		
		if(lat)
			return r1.getPos().getLatitude();
		else
			return r1.getPos().getLongitude();
	}
}
	
	/*
	public void trainGridMap(AisMessage msg){
		
	}*/
