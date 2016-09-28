package lsde10.suspicious.outage;

public class Track {
	//public int mmsi;
	public float longtitude;
	public float latitude;
	public int timestamp;
	
	public Track(float latit, float longt, int time)
	{
		//mmsi = ship_mmsi;
		longtitude = longt;
		latitude = latit;
		timestamp = time;
	}	
	
	/*public int getMmsi() {
		return mmsi;
	}
	public void setMmsi(int mmsi) {
		this.mmsi = mmsi;
	}*/
	
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
	public int getTime() {
		return timestamp;
	}
	public void setTime(int time) {
		this.timestamp = time;
	}	
}
