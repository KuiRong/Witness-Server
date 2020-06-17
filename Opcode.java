package java_server;

import java.util.Collections;
import java.util.Date;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;

public class Opcode {
	private static Logger logger = Logger.getLogger(Opcode.class);
	public static final long HEARTBEAT_TIMEDOUT = 10000;
			
	public static void heartBeat(Long ApplianceId, Long nodeId, Short master,short lockOwner) {
		boolean Master = false;
		if(master == 1) {
			Master = true;
		}else {
			Master = false;
		}
		String lkOwner = null;	//20200526	����n��==>�w��
//		logger.info("CServer.judgeDone is " + CServer.judgeDone);
//		logger.info("lockOwner  is " + lockOwner);
		if(CServer.judgeDone) {
			lkOwner = CServer.nodeInfo.get(Long.toString(nodeId)).lockOwner.split(":")[1];
//			logger.info("nodeId " + nodeId +" is " + lkOwner);
		}
		else if (lockOwner == 0) {	//	20200611	�ثehearbeat�u�|��0��server�A�|�ק�줧�᪺�欰(race condition)==> requirelock ���� set CServer.judgeDone�i�H��
			lkOwner = "false";
		}
		else if (lockOwner == 1) {
			lkOwner = "true";
		}

		if (!CServer.nodeInfo.containsKey(Long.toString(nodeId))) {
			CSetInfo info;
			info = new CSetInfo(Long.toString(ApplianceId), Long.toString(nodeId), String.valueOf(Master) ,lkOwner);
			CSetInfo.setData(Long.toString(nodeId), info);
		}else {
			CServer.nodeInfo.get(Long.toString(nodeId)).setNodeId(Long.toString(nodeId));
			CServer.nodeInfo.get(Long.toString(nodeId)).setAP(Long.toString(ApplianceId));
			CServer.nodeInfo.get(Long.toString(nodeId)).setMaster(String.valueOf(Master));
			CServer.nodeInfo.get(Long.toString(nodeId)).setLastHeartBeat();
			CServer.nodeInfo.get(Long.toString(nodeId)).initLockOwner(lkOwner);
		}

		logger.debug("[heartbeat] reqid : " + nodeId);
		updateTable();	//20200522 �g���
		logger.debug("Table update done!");
	}
	
	public static void scanAllUnLockAlready() {
		// TODO Auto-generated method stub
		Iterator<String> scanIt = CServer.nodeInfo.keySet().iterator();
		while (scanIt.hasNext()) {
			String scanKey = scanIt.next();
			if(Boolean.valueOf(CServer.nodeInfo.get(scanKey).lockOwner.split(":")[1])) {
				logger.info("can not unlock becaz some node have lock!!.");
				break;
			}
		}
		if (!scanIt.hasNext()) {
			CServer.judgeDone = false;
			logger.info("unset all lockOwner!!.");
		}
	}
	
	public static void scanLockAlready(){
		Iterator<String> scanIt = CServer.nodeInfo.keySet().iterator();
		if(!CServer.judgeDone) {
			while (scanIt.hasNext()) {
				Object scanKey = scanIt.next();
				if(Boolean.valueOf(CServer.nodeInfo.get(scanKey).lockOwner.split(":")[1])) {
					logger.info("Already have lockOwner!!");
					CServer.judgeDone = true;
					break;
				}
			}
		}
	}
	
	public static void judge(){
		Iterator<String> it = CServer.nodeInfo.keySet().iterator();
		Date now = new Date();
		int apCount = 0;
		int totalApCount = 0;
		String setApRequireLock = null;
		String maxKey = null;
		long objTimeout;
		
		while (it.hasNext()) {			//20200525 �ˬdCServer.nodeInfo�Ҧ������
			String key = it.next();			//20200522 nodeId
			objTimeout = now.getTime() - CServer.nodeInfo.get(key).lastHeartBeat.getTime();
			logger.debug("objTimeout is :" + objTimeout + "s");
			if(objTimeout > HEARTBEAT_TIMEDOUT) {
				// unset this object data or ignore this object ==> ignore this object
				logger.debug("Node " + CServer.nodeInfo.get(key).nodeId +"is failure.");
				continue;
			}else {						//20200529	��ƨS�L���N�έpAP���U��node�ƶq
				if(!CServer.applianceCount.containsKey((CServer.nodeInfo.get(key).apId))) {	//	20200522	CServer.applianceCount �@�}�l�O�Ū�, key ���s�b
					CServer.applianceCount.put(CServer.nodeInfo.get(key).apId, 1);	//20200522	format = {"apId:1111" : 1}
					totalApCount++;
				}else {
					apCount = CServer.applianceCount.get(CServer.nodeInfo.get(key).apId) + 1;
					CServer.applianceCount.put(CServer.nodeInfo.get(key).apId, apCount);
					apCount = 0;	//20200522 �n���@�U���S��bug,�γ\hashmap��obj��value�|�Q�M��0	���LInteger�O�򥻸�ƫ��A  �ҥH���T�w
					totalApCount++;
				}
			}
		}
		//compare node count
		//if ==> ap count same
		Iterator<String> scanNodeInfo = CServer.nodeInfo.keySet().iterator();
		if(totalApCount == 0) {
			logger.info("ALL CServer.nodeInfo is Expired !!!");
		}
		else if((totalApCount % 2) == 0) {
			while(scanNodeInfo.hasNext()) {
				Object key = scanNodeInfo.next();
				logger.info("CServer.nodeInfo.get(key).master is " + CServer.nodeInfo.get(key).master);
				if(CServer.nodeInfo.get(key).master.equals("master:true")) {	//20200525	���master�Ҧb��AP
					setApRequireLock = new String(CServer.nodeInfo.get(key).apId); //20200522	format = {nodeId : "apId:1"} ==> setApRequireLock = "apId:1"
					break;
				}
			}
		//else ==> ap count diff
		}else {	//�ثe�N���AP ���T�ӥH�W�N�ݭn��P�_
			logger.info("CServer.applianceCount.keySet() is " + CServer.applianceCount.keySet());
			maxKey = Collections.max(CServer.applianceCount.keySet());
			logger.info("Max Appliance is" + maxKey);
			setApRequireLock = new String(maxKey);
		}
		now = null;
		it = null;
		maxKey = null;
		scanNodeInfo = null;
		if(setApRequireLock == null) {
			logger.info("No need to update table!!!");
		}else {
			setLockToNodeInfo(setApRequireLock);
			updateTable();	//20200522 �g���
		}
	}
	
	public static void setLockToNodeInfo(String ApRequireLock){
		Iterator<String> iter = CServer.nodeInfo.keySet().iterator();
		Date now = new Date();
		long objTimeout;
		while(iter.hasNext()) {
			Object nodeIdkey = iter.next();
			objTimeout = now.getTime() - CServer.nodeInfo.get(nodeIdkey).lastHeartBeat.getTime();
			if(objTimeout > HEARTBEAT_TIMEDOUT) {
				// unset this object data or ignore this object ==> ignore this object
				logger.info("Node " + CServer.nodeInfo.get(nodeIdkey).nodeId +"is failure.");
				continue;
			}
			else if(CServer.nodeInfo.get(nodeIdkey).apId.equals(ApRequireLock)) {	//20200525	��master�P�@��AP��node�]�|����lock
				CServer.nodeInfo.get(nodeIdkey).lockOwner = new String("lockOwner:true");
				logger.info("Node: " + CServer.nodeInfo.get(nodeIdkey).nodeId +" set lock.");
				CServer.judgeDone = true;
			}
		}
		ApRequireLock = null;
		CServer.applianceCount.clear();
	}
	
	public static void updateTable(){
		try
		{
			File gluster= new File("./gluster.txt");
			BufferedWriter bw = new BufferedWriter(new FileWriter(gluster));
			Iterator<String> it = CServer.nodeInfo.keySet().iterator();
			while (it.hasNext()) {
				Object key = it.next();
				bw.write("\r\n");
				
				bw.write(CServer.nodeInfo.get(key).nodeId);
				//CServer.nodeInfo.get(key).nodeId = "nodeId:1"
				bw.write(" ");
				bw.write(CServer.nodeInfo.get(key).apId);
				bw.write(" ");
				bw.write(CServer.nodeInfo.get(key).master);
				bw.write(" ");
				bw.write(CServer.nodeInfo.get(key).lockOwner);
			}
			
		bw.close();
		logger.debug("Write table (file) done!");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
