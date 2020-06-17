package java_server;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.log4j.Logger;

public class Command {
	private static Logger logger = Logger.getLogger(Command.class);
	/*Witness OP code*/
	public static final short WITNESS_OP_NOT_DEF		         = 0x00;
	public static final short WITNESS_OP_GET_VER_REQ			 = 0x10;
	public static final short WITNESS_OP_GET_VER_RSP 		     = 0x11;
	
	public static final short WITNESS_OP_SET_CLUSTER_DATA_REQ 	 = 0x20;
	public static final short WITNESS_OP_SET_CLUSTER_DATA_RSP 	 = 0x21;
	
	public static final short WITNESS_OP_GET_CLUSTER_DATA_REQ 	 = 0x22;
	public static final short WITNESS_OP_GET_CLUSTER_DATA_RSP 	 = 0x23;
	
	public static final short WITNESS_OP_REQUIRE_LOCK_REQ		 = 0x30;
	public static final short WITNESS_OP_REQUIRE_LOCK_RSP 		 = 0x31;
	
	public static final short WITNESS_OP_HEARTBEAT_REQ			 = 0x40;
	public static final short WITNESS_OP_HEARTBEAT_RSP			 = 0x41;
	
	public static final short WITNESS_OP_REQUIRE_UNLOCK_REQ		 = 0x50;
	public static final short WITNESS_OP_REQUIRE_UNLOCK_RSP		 = 0x51;
	
	/*Witness Response Code*/
	public static final int WITNESS_STATUS_SUCCESS           	 = 0x0;
	public static final int WITNESS_STATUS_NOT_READY         	 = 0x1;
	public static final int WITNESS_STATUS_INVALID_gluster_id 	 = 0x2;
	public static final int WITNESS_STATUS_INVALID_NODEID	     = 0x3;
	public static final int WITNESS_STATUS_INVALID_VERSION   	 = 0x4;
	public static final int WITNESS_STATUS_INVALID_DATA      	 = 0x5;
	public static final int WITNESS_STATUS_INVALID_GENERATION    = 0x6;
	public static final int WITNESS_STATUS_LOCKED	             = 0x7;
	public static final int WITNESS_STATUS_CRC_ERROR			 = 0x8;
	public static final int WITNESS_STATUS_ERROR             	 = 0xff;
	
	public static final long HEARTBEAT_TIMEDOUT = 10000;
	
	public short lockOwner;
	public short Master;
	public short status;
	public int crc;
	public short op;
	public short length;
	public Long applianceId;
	public Long nodeId;
	public Long generation;
	
	private Boolean isDataVaild;
	
	public byte[] data;
	
	public static final int[] table = {
            0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
            0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
            0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
            0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
            0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
            0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
            0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
            0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
            0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
            0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
            0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
            0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
            0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
            0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
            0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
            0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
            0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
            0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
            0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
            0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
            0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
            0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
            0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
            0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
            0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
            0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
            0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
            0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
            0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
            0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
            0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
            0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040,
        };
	
	
	public Command(byte[] header)
	{
		int headerCrc;
		byte[] tmp = header;
		this.data = new byte[0];
		
		lockOwner = getShort(Arrays.copyOfRange(tmp, 8, 10));
		crc = getShortCRC(Arrays.copyOfRange(tmp, 10, 12));			//20200514 複製10~11
		op = getShort(Arrays.copyOfRange(tmp, 12, 14));
		length = getShort(Arrays.copyOfRange(tmp, 14, 16));
		applianceId = getLong(Arrays.copyOfRange(tmp, 16, 24));
		nodeId = getLong(Arrays.copyOfRange(tmp, 24, 32));
		generation = getLong(Arrays.copyOfRange(tmp, 32, 40));
		status = getShort(Arrays.copyOfRange(tmp, 40, 42));
		Master = getShort(Arrays.copyOfRange(tmp, 42, 44));
		header[10] = 0;
		header[11] = 0;
		headerCrc = getCrc(header, WitnessReceiver.HEADER_LENGTH);
		isDataVaild = headerCrc == crc;
	}
	
	public int response(OutputStream bw)
	{
		switch(op)
		{
			case WITNESS_OP_REQUIRE_LOCK_REQ:
				logger.debug("entry WITNESS_OP_REQUIRE_LOCK_REQ case");
			    return this.requireLock(bw);
			case WITNESS_OP_REQUIRE_UNLOCK_REQ:
				logger.debug("entry WITNESS_OP_REQUIRE_UNLOCK_REQ case");
				return this.requireUnLock(bw);
			case WITNESS_OP_HEARTBEAT_REQ:
				logger.debug("entry WITNESS_OP_HEARTBEAT_REQ case");
				return this.heartbeat(bw);
		}
		
		return this.unknownCommand(bw);
	}

	
	private synchronized int requireUnLock(OutputStream bw) {
		// TODO Auto-generated method stub
		byte[] buf = new byte[WitnessReceiver.HEADER_LENGTH];
		String nodeIdUnLockKey = Long.toString(nodeId);
		Date now = new Date();
		long objTimeout;
		objTimeout = now.getTime() - CServer.nodeInfo.get(nodeIdUnLockKey).lastHeartBeat.getTime();
		
		logger.debug("[requireUnLock] reqid : " + nodeId);
		if(CServer.judgeDone == false) {
			logger.info("unlock already");
		}
		else if(objTimeout > HEARTBEAT_TIMEDOUT) {
			// unset this object data or ignore this object ==> ignore this object
			logger.info("Node " + CServer.nodeInfo.get(nodeIdUnLockKey).nodeId +"is failure.");
		}
		else if(Boolean.valueOf(CServer.nodeInfo.get(nodeIdUnLockKey).lockOwner.split(":")[1])) {
			CServer.nodeInfo.get(nodeIdUnLockKey).initLockOwner("false");
			logger.debug("Set nodeId "+ nodeId +" key(lockOwner) value to 'false'");
			Opcode.updateTable();
			Opcode.scanAllUnLockAlready();
		}else {
			logger.debug("The key(lockOwner) of nodeId "+ nodeId +" value is 'false' already!");
		}
		
		this.initBuffer(buf, WITNESS_OP_REQUIRE_UNLOCK_RSP);

		return sendBuf(bw, buf);
	}

	//20200522	4個node  4 個 thread 都會來要lock
	private synchronized int requireLock(OutputStream bw) {			//20200521 用這個準沒錯
		byte[] buf = new byte[WitnessReceiver.HEADER_LENGTH];
		String nodeIdKey = Long.toString(nodeId);

		logger.info("[requireLock] reqid : " + nodeId);
		
		Opcode.scanLockAlready();
		if(CServer.judgeDone) {										//20200525	必須要等到recover的request來才可以set成false,可能還要寫成table(file)
			logger.info("already set lock");
		}else {
			logger.info("judge appliance lock");		
			Opcode.judge();			//multiple thread will entry need lock
		}

		this.initBuffer(buf, WITNESS_OP_REQUIRE_LOCK_RSP);
		logger.info("nodeIdKey is " + nodeIdKey);
		logger.info("Boolean.valueOf(CServer.nodeInfo.get(nodeIdKey).lockOwner.split(\":\")[1]) is " + Boolean.valueOf(CServer.nodeInfo.get(nodeIdKey).lockOwner.split(":")[1]));
		if(Boolean.valueOf(CServer.nodeInfo.get(nodeIdKey).lockOwner.split(":")[1])) {	//20200521 好像沒有送出去 ==> 20200525 送了
			System.arraycopy(toByteArray(1), 0, buf, 8, 2);
		}
		
		return sendBuf(bw, buf);
	}
	
	public synchronized int heartbeat(OutputStream bw)
	{
		byte[] buf = new byte[WitnessReceiver.HEADER_LENGTH];		//Create buffer 長度48
		this.initBuffer(buf, WITNESS_OP_HEARTBEAT_RSP);
//		System.arraycopy(Command.toByteArray(WITNESS_STATUS_SUCCESS), 0, buf, 40, 4);
		// 已上 init 完 buffer
		Opcode.heartBeat(applianceId, nodeId, Master, lockOwner);
		return sendBuf(bw, buf);
	}
	
	public int unknownCommand(OutputStream bw)
	{
		byte[] buf = new byte[WitnessReceiver.HEADER_LENGTH];
		this.initBuffer(buf, WITNESS_OP_NOT_DEF);
		System.arraycopy(Command.toByteArray(WITNESS_STATUS_ERROR), 0, buf, 40, 4);
		return sendBuf(bw, buf);
	}
	
//	private int crcError(OutputStream bw) {
//		byte[] buf = new byte[WitnessReceiver.HEADER_LENGTH];
//		this.initBuffer(buf, WITNESS_OP_NOT_DEF);
//		System.arraycopy(Command.toByteArray(WITNESS_STATUS_CRC_ERROR), 0, buf, 40, 4);
//		return sendBuf(bw, buf);
//	}

	private synchronized int sendBuf(OutputStream bw, byte[] buf)
	{
		try {
			int crc = getCrc(buf, WitnessReceiver.HEADER_LENGTH);
			
			System.arraycopy(toByteArray(crc), 0, buf, 10, 2);
			
//			printByte(buf);
			
			for (byte b : buf)
				bw.write(b);
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
		return 0;
	}
	
	private synchronized void initBuffer(byte[] buf, short opcod)
	{	//20200520	buffer init 的過程是使用大小計算
		short crc = 0;
		short status = 0;
		// 20200514	System.arraycopy(來源, 起始索引, 目的, 起始索引, 複製長度)
		// 20200514	getBytes()將字串轉成byte ==> 將結果存入新的 byte array中
		// 20200514	WitnessReceiver.MAGIC.length() 長度是8
		System.arraycopy(WitnessReceiver.WITNESS_MAGIC.getBytes(), 0, buf, 0, WitnessReceiver.MAGIC.length());
		System.arraycopy(toByteArray(lockOwner), 0, buf, 8, 2);

		System.arraycopy(toByteArray(crc), 0, buf, 10, 2);
		System.arraycopy(toByteArray(opcod), 0, buf, 12, 2);
		System.arraycopy(toByteArray(applianceId), 0, buf, 16, 8);
		System.arraycopy(toByteArray(nodeId), 0, buf, 24, 8);
		System.arraycopy(WitnessReceiver.WITNESS_MAGIC.getBytes(), 0, buf, 32, 8);
//		logger.info("[initBuffer]========================");
//		logger.info("CServer.nodeInfo.keySet() is ==> " + CServer.nodeInfo.keySet()); //[1111,2222,3333,4444]
//		System.out.println("nodeId is "+ nodeId);
//		logger.info("Node: " + CServer.nodeInfo.get(String.valueOf(nodeId))+ " opecode is: " + opcod);
		if(opcod == WITNESS_OP_REQUIRE_UNLOCK_RSP) {
			logger.info("[REQUIRE_UNLOCK_RSP]========================");
			logger.info("[REQUIRE_UNLOCK_RSP] nodeId is "+ nodeId);
			logger.info("\n");
			status = 5;
		}else if(opcod == WITNESS_OP_REQUIRE_LOCK_RSP) {
			logger.info("[REQUIRE_LOCK_RSP]========================");
			if(CServer.nodeInfo.get(String.valueOf(nodeId)).lockOwner.equals("lockOwner:true")) {	//20200525	跟master同一個AP的node也會拿到lock
				logger.info("[REQUIRE_LOCK_RSP] nodeId is "+ nodeId);
				logger.info("\n");
				System.arraycopy(toByteArray(1), 0, buf, 8, 2);
			}
			status = 4;
		}else if(opcod == WITNESS_OP_HEARTBEAT_RSP) {
//			logger.info("[HEARTBEAT_RSP]========================");
			if(CServer.nodeInfo.get(String.valueOf(nodeId)).lockOwner.equals("lockOwner:true")) {	//20200525	跟master同一個AP的node也會拿到lock
//				logger.info("[HEARTBEAT_RSP] nodeId is "+ nodeId);
			}
//			logger.info("[HEARTBEAT_RSP] set status = 3");
			status = 3;
		}
		System.arraycopy(toByteArray(status), 0, buf, 40, 2);
	}

//	toByteArray 多載
	
	public static byte[] toByteArray(short num)
	{
		byte[] bytes = ByteBuffer.allocate(2).array();
		for (int i = 0; i < 2; i++)
			bytes[i] = (byte) ((num >> i*8) & 0xff);
		return bytes;
	}
	
	public static byte[] toByteArray(int num)
	{
		byte[] bytes = ByteBuffer.allocate(4).array();
		for (int i = 0; i < 4; i++)
			bytes[i] = (byte) ((num >> i*8) & 0xff);
		return bytes;
	}
	
	public static byte[] toByteArray(long num)
	{
		byte[] bytes = ByteBuffer.allocate(8).array();
		for (int i = 0; i < 8; i++)
			bytes[i] = (byte) ((num >> i*8) & 0xff);
		return bytes;
	}
	
	public void printState()
	{
		System.out.println("Major: " + lockOwner);
		System.out.println("crc: " + crc);
		System.out.println("op: " + op);
		System.out.println("length: " + length);
		System.out.println("applianceId: " + applianceId);
		System.out.println("nodeId: " + nodeId);
		System.out.println("generation: " + generation);
	}
	
	private int getShortCRC(byte[] buf)
	{
		int ret = 0;
		int tmp;
		for (int i = 1; i >= 0; i--)
		{
			tmp = buf[i] & 0xff;
			ret *= 256;
			ret += tmp;
		}
		return ret & 0xffff;
	}
	
	private short getShort(byte[] buf)
	{
		short ret = 0;
		int tmp;
		for (int i = 1; i >= 0; i--)
		{
			tmp = buf[i] & 0xff;
			ret *= 256;
			ret += tmp;
		}
		return ret;
	}
	
	public static int getInt(byte[] buf)
	{
		int ret = 0;
		int tmp;
		for (int i = 3; i >= 0; i--)
		{
			tmp = buf[i] & 0xff;
			ret *= 256;
			ret += tmp;
		}
		
		return ret;
	}
	
	public static long getLong(byte[] buf)
	{
		long ret = 0;
		long tmp;
		for (int i = 7; i >= 0; i--)
		{
			tmp = buf[i] & 0xff;
			tmp = tmp << 8*i; 
			ret |= tmp;
		}
		
		return ret;
	}
	
//	CRC多載
	public static int getCrc(byte[] header, int headerLength)
	{
		int crc = 0x0000;
		for (int i = 0; i < headerLength; i++)
            crc = ((crc >>> 8) ^ table[(crc ^ header[i]) & 0xff]);
		
		return (crc & (0xffff));
	}
	
	public static int getCrc(char[] header, int headerLength)
	{
		int crc = 0x0000;
		for (int i = 0; i < headerLength; i++)
            crc = ((crc >>> 8) ^ table[(crc ^ header[i]) & 0xff]);
		
		return (crc & (0xffff));
	}
}
