package lsde10.suspicious.outage;

import org.apache.spark.Accumulable;
import org.apache.spark.AccumulableParam;

public class GridMap extends Accumulable<Double, Integer> {

	public GridMap(Double initialValue, AccumulableParam<Double, Integer> param) {
		super(initialValue, param);
		// TODO Auto-generated constructor stub
	}
}
