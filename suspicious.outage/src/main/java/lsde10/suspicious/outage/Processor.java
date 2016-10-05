package lsde10.suspicious.outage;

import java.util.ArrayList;
import java.util.logging.Logger;

import dk.tbsalling.aismessages.ais.messages.AISMessage;
import dk.tbsalling.aismessages.ais.messages.Metadata;
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
			ais = this.decode(nmea);
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
	
	public void trainGridMap(AISMessage msg){
		
	}
	
	
	 public AISMessage decode(NMEAMessage nmeaMessage) {
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
		}


}
