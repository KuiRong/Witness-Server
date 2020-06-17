package java_server;

import java.util.Date;

import org.apache.log4j.Logger;

public class CSetInfo {
	private static Logger logger = Logger.getLogger(CSetInfo.class);
	String nodeId;
	String apId;
	String master;
	Date lastHeartBeat;
	String lockOwner;

	public CSetInfo(String aId, String nId, String master, String lkOwner) {
		this.apId = "apId:" + aId;
		this.nodeId = "nodeId:" + nId;
		this.master = "master:" + master;
		this.lastHeartBeat = new Date();
		this.lockOwner = "lockOwner:" + lkOwner;
	}
	public CSetInfo(String aId, String nId, String master, String lkOwner, String test) {
		this.apId = "apId:" + aId;
		this.nodeId = "nodeId:" + nId;
		this.master = "master:" + master;
		if(this.lastHeartBeat == null) {
			this.lastHeartBeat = new Date();
		}else {
			logger.debug("this.lastHeartBeat will do nothing");
		}
		this.lockOwner = "lockOwner:" + lkOwner;		
	}
	public void setAP(String aId) {
		this.apId = "apId:" + aId;
	}
	public void setNodeId(String nId) {
		this.nodeId = "nodeId:" + nId;
	}
	public void setMaster(String master) {
		this.master = "master:" + master;
	}
	public void setLastHeartBeat() {
		this.lastHeartBeat = null;			//20200522	release memory
		this.lastHeartBeat = new Date();
	}
	public void initLockOwner(String lkOwner) {
		this.lockOwner = "lockOwner:" + lkOwner;
	}
	
//	Ãþ§O¨ç¦¡
	public static void setData(String nId, CSetInfo info) {
		CServer.nodeInfo.put(nId, info);
	}
}
