package lsde10.suspicious.outage;

import java.util.List;

import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.api.java.JavaRDD;

public class Grid extends Accumulator<Integer> {	
	
	List<Integer> mmsi;
	public float lat;
	public float lon;
	public int size;
	

	public Grid(Integer initialValue, AccumulatorParam<Integer> param, int mmsi, float lat, float lon, int size) {
		super(initialValue, param);
		this.lat = lat;
		this.lon = lon;
		this.size = size;
		this.mmsi = null;
	}
	
	public void addNewMmsi(int new_mmsi)
	{
		this.mmsi.add(new_mmsi);
	}

	public List<Integer> getMmsi() {
		return mmsi;
	}

	@Override
	public void add(Integer term) {
		// TODO Auto-generated method stub
		super.add(term);
	}
}
