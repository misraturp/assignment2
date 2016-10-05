package lsde10.suspicious.outage;

import java.util.ArrayList;
import java.util.logging.Logger;

import dk.tbsalling.aismessages.ais.messages.AISMessage;
import dk.tbsalling.aismessages.ais.messages.Metadata;
import dk.tbsalling.aismessages.ais.messages.PositionReportClassAAssignedSchedule;
import dk.tbsalling.aismessages.ais.messages.PositionReportClassAResponseToInterrogation;
import dk.tbsalling.aismessages.ais.messages.PositionReportClassAScheduled;
import dk.tbsalling.aismessages.nmea.NMEAMessageHandler;
import dk.tbsalling.aismessages.nmea.NMEAMessageInputStreamReader;
import dk.tbsalling.aismessages.nmea.exceptions.InvalidMessage;
import dk.tbsalling.aismessages.nmea.exceptions.NMEAParseException;
import dk.tbsalling.aismessages.nmea.exceptions.UnsupportedMessageType;
import dk.tbsalling.aismessages.nmea.messages.NMEAMessage;

public class Processor {

	private static final Logger log = Logger.getLogger(NMEAMessageInputStreamReader.class.getName());
	private static final Logger LOG = Logger.getLogger(NMEAMessageHandler.class.getName());
	private final ArrayList<NMEAMessage> messageFragments = new ArrayList<>();
	

	private static Processor instance = null;

	private Processor() {
	}

	public static Processor getInstance() {
		if (instance == null) {
			instance = new Processor();
		}
		return instance;
	}
	
	public String cleanAISMsg(String msg){
		if(!msg.startsWith("!")){
			int index = msg.indexOf("!", 0);
			return msg.substring(index);
		}
		return msg;
	}
	
	
	public AISMessage decodeAISMessage(String msg) {

		AISMessage ais = null;
		try {
			NMEAMessage nmea = NMEAMessage.fromString(msg);
			//ais = this.decode(nmea);
			log.fine("Received: " + nmea.toString());
		} catch (InvalidMessage invalidMessageException) {
			log.warning("Received invalid AIS message: \"" + msg + "\"");
		} catch (UnsupportedMessageType unsupportedMessageTypeException) {
			log.warning("Received unsupported NMEA message: \"" + msg + "\"");
		} catch (NMEAParseException parseException) {
			log.warning("Received non-compliant NMEA message: \"" + msg + "\"");
		}
		
		
		return ais;
	}
	
	public AISMessage compareLattitude (AISMessage msg1, AISMessage msg2, boolean max)
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
	
	public AISMessage compareLongtitude (AISMessage msg1, AISMessage msg2, boolean max)
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
	
	public void trainGridMap(AISMessage msg){
		
	}
	
	
	 /*public AISMessage decode(NMEAMessage nmeaMessage) {
			LOG.finer("Received for processing: " + nmeaMessage.getRawMessage());
			
			if (! nmeaMessage.isValid()) {
				LOG.warning("NMEA message is invalid: " + nmeaMessage.toString());
				return null;
			}
			
			int numberOfFragments = nmeaMessage.getNumberOfFragments();
			if (numberOfFragments <= 0) {
				LOG.warning("NMEA message is invalid: " + nmeaMessage.toString());
				messageFragments.clear();
			} else if (numberOfFragments == 1) {
				LOG.finest("Handling unfragmented NMEA message");
	            AISMessage aisMessage = AISMessage.create(new Metadata(source), nmeaMessage);
				messageFragments.clear();
				return aisMessage;
			} else {
				int fragmentNumber = nmeaMessage.getFragmentNumber();
				LOG.finest("Handling fragmented NMEA message with fragment number " + fragmentNumber);
				if (fragmentNumber < 0) {
					LOG.warning("Fragment number cannot be negative: " + fragmentNumber + ": " + nmeaMessage.getRawMessage());
					messageFragments.clear();
				} else if (fragmentNumber > numberOfFragments) {
					LOG.fine("Fragment number " + fragmentNumber + " higher than expected " + numberOfFragments + ": " + nmeaMessage.getRawMessage());
					messageFragments.clear();
				} else {
					int expectedFragmentNumber = messageFragments.size() + 1;
					LOG.finest("Expected fragment number is: " + expectedFragmentNumber + ": " + nmeaMessage.getRawMessage());
					
					if (expectedFragmentNumber != fragmentNumber) {
						LOG.fine("Expected fragment number " + expectedFragmentNumber + "; not " + fragmentNumber + ": " + nmeaMessage.getRawMessage());
						messageFragments.clear();
					} else {
						messageFragments.add(nmeaMessage);
						LOG.finest("nmeaMessage.getNumberOfFragments(): " + nmeaMessage.getNumberOfFragments());
						LOG.finest("messageFragments.size(): " + messageFragments.size());
						if (nmeaMessage.getNumberOfFragments() == messageFragments.size()) {
	                        AISMessage aisMessage = AISMessage.create(new Metadata(source), messageFragments.toArray(new NMEAMessage[messageFragments.size()]));
							messageFragments.clear();
						} else
							LOG.finest("Fragmented message not yet complete; missing " + (nmeaMessage.getNumberOfFragments() - messageFragments.size()) + " fragment(s).");
					}
				}
			}
		}*/


}
