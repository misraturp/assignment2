package lsde10.suspicious.outage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import dk.tbsalling.aismessages.AISInputStreamReader;

public class SuspiciousOutage {

	public static void main( String[] args )
    {
		String path = System.getProperty("user.dir");
		File data = new File(path + "//data//03//06-00.txt");
		InputStream inputStream = null;
		
		try {
			inputStream = new FileInputStream(data);
			AISInputStreamReader streamReader
        	= new AISInputStreamReader(
        			inputStream,
                    aisMessage -> System.out.println(aisMessage));
        streamReader.run();
        
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
    }

}
