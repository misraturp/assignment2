package lsde10.suspicious.outage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import dk.tbsalling.aismessages.AISInputStreamReader;
import dk.tbsalling.aismessages.ais.messages.AISMessage;
import scala.Tuple2;

public class Processor {

	private static Processor instance = null;

	private Processor() {
	}

	public static Processor getInstance() {
		if (instance == null) {
			instance = new Processor();
		}
		return instance;
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
	
	
	public List<AISMessage> decodeAISMessage(Tuple2<String, String> file) {
		List<AISMessage> ret = new ArrayList<AISMessage>();
		String content = file._2();
		InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

		AISInputStreamReader streamReader
    	= new AISInputStreamReader(
    			stream,
                aisMessage -> ret.add(aisMessage));
    	
    	try {
			streamReader.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return ret;
	}
	
	
	
	public void trainGridMap(AISMessage msg){
		
	}
	
}
