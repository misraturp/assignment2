package lsde10.suspicious.outage;

import java.io.Serializable;
import java.util.Date;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;

public class Track implements Serializable{
	//public int mmsi;
	private float longtitude;
	private float latitude;
	private Date timestamp;
	boolean startOut = false;
	boolean endOut = false;	
	
	public Track(float longtitude, float latitude, Date timestamp) 
	{
		this.longtitude = longtitude;
		this.latitude = latitude;
		this.timestamp = timestamp;
	}
	
	/*public int getMmsi() {
		return mmsi;
	}
	public void setMmsi(int mmsi) {
		this.mmsi = mmsi;
	}*/
	
	public boolean isStartOut() {
		return startOut;
	}

	public void setStartOut(boolean startOut) {
		this.startOut = startOut;
	}

	public boolean isEndOut() {
		return endOut;
	}

	public void setEndOut(boolean endOut) {
		this.endOut = endOut;
	}
	
	public float getLongtitude() {
		return longtitude;
	}

	public void setLongtitude(float longtitude) {
		this.longtitude = longtitude;
	}
	public float getLatitude() {
		return latitude;
	}
	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}
	public Date getTime() {
		return timestamp;
	}
	public void setTime(Date time) {
		this.timestamp = time;
	}	
}
