package lsde10.suspicious.outage;

import dk.dma.ais.message.AisMessage;

public class AisWithTime {
	
	private AisMessage msg;
	private int time;
	
	public AisWithTime(AisMessage msg,int time){
		this.setTime(time);
		this.setMsg(msg);
	}
	
	public int getTime() {
		return time;
	}

	public void setTime(int time) {
		this.time = time;
	}

	public AisMessage getMsg() {
		return msg;
	}

	public void setMsg(AisMessage msg) {
		this.msg = msg;
	}

}
