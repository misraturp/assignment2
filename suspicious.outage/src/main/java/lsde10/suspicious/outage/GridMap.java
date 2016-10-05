package lsde10.suspicious.outage;

import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;
import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;

public class GridMap extends Accumulator<Integer> {

	
	
	public GridMap(Integer initialValue, AccumulatorParam<Integer> param) {
		super(initialValue, param);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Integer term) {
		// TODO Auto-generated method stub
		super.add(term);
	}
}
