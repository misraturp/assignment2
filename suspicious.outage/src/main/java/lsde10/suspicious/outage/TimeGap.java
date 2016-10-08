package lsde10.suspicious.outage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TimeGap {
	
	private AisWithTime begin;
	private AisWithTime end;
	
	public TimeGap(AisWithTime begin, AisWithTime end) {
		this.setBegin(begin);
		this.setEnd(end);
	}

	
	/*public TimeGap computeNew(TimeGap one, TimeGap two){
		List<AisWithTime> times = new ArrayList<AisWithTime>();
		
		times.sort(new Comparator<AisWithTime>() {

			@Override
			public int compare(AisWithTime o1, AisWithTime o2) {
				if(o1.getTime() < o2.getTime())
					return -1;
				else if(o1.getTime() == o2.getTime())
					return 0;
				return 1;
			}
		});
		
		int gap1 = times.get(1).getTime() - times.get(0).getTime();
		int gap2 = times.get(2).getTime() - times.get(1).getTime();
		int gap3 = times.get(3).getTime() - times.get(2).getTime();
		
	}*/
	
	
	
	public AisWithTime getBegin() {
		return begin;
	}

	public void setBegin(AisWithTime begin) {
		this.begin = begin;
	}

	public AisWithTime getEnd() {
		return end;
	}

	public void setEnd(AisWithTime end) {
		this.end = end;
	}
	
	
	
	
}
