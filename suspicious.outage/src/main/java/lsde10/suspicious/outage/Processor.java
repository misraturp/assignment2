package lsde10.suspicious.outage;

import java.util.logging.Logger;

import dk.tbsalling.aismessages.ais.messages.AISMessage;
import dk.tbsalling.aismessages.nmea.NMEAMessageHandler;
import dk.tbsalling.aismessages.nmea.NMEAMessageInputStreamReader;
import dk.tbsalling.aismessages.nmea.exceptions.InvalidMessage;
import dk.tbsalling.aismessages.nmea.exceptions.NMEAParseException;
import dk.tbsalling.aismessages.nmea.exceptions.UnsupportedMessageType;
import dk.tbsalling.aismessages.nmea.messages.NMEAMessage;

public class Processor {

	private static final Logger log = Logger.getLogger(NMEAMessageInputStreamReader.class.getName());
	private NMEAMessageHandler nmeaMessageHandler;

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
		//TODO
		
		return " ";
	}
	
	
	public AISMessage decodeAISMessage(String msg) {

		// this.nmeaMessageHandler = new NMEAMessageHandler("SRC",
		// aisMessageConsumer);

		String string = " ";
		try {
			NMEAMessage nmea = NMEAMessage.fromString(msg);
			NMEAMessageHandler nmeaMessageHandler;
			// nmeaMessageHandler.accept(nmea);
			log.fine("Received: " + nmea.toString());
		} catch (InvalidMessage invalidMessageException) {
			log.warning("Received invalid AIS message: \"" + string + "\"");
		} catch (UnsupportedMessageType unsupportedMessageTypeException) {
			log.warning("Received unsupported NMEA message: \"" + string + "\"");
		} catch (NMEAParseException parseException) {
			log.warning("Received non-compliant NMEA message: \"" + string + "\"");
		}
		return null;
	}
	
	public void trainGridMap(AISMessage msg){
		
	}

}
