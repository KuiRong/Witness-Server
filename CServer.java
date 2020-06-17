package java_server;

import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;


public class CServer extends Thread{

	private int port;
	private Boolean isRunning = false;
	private ServerSocket globalServerSocket;
//	private CUpDateByHand globalUpDateTable;	//for change connect table by user
	private HashSet<WitnessReceiver> serverReceiver;
	private static Logger logger = Logger.getLogger(CServer.class);  

	
	static HashMap<String, CSetInfo> nodeInfo = new HashMap<String, CSetInfo>();
	static HashMap<String, Integer> applianceCount = new HashMap<String, Integer>();
	
	static boolean judgeDone = false;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CServer server = new CServer(8000);	//20200513	建一個名為w的Witness的物件
		server.init();
		server.runServerThread();

	}
	public CServer(int port)
	{
		this.port = port;
	}
	
	public CServer()
	{
		this.port = 8000;
	}
	
	public void init()
	{
		this.serverReceiver = new HashSet<WitnessReceiver>();	//20200513	建一個存WitnessReceiver物件的hashset
		try {
			this.initWitness();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void initWitness() throws FileNotFoundException
	{
		File witnessTable = new File("./gluster.txt");
		Long nodeId = null;
		Long ApId = null;
		String master = null;
		String lockOwner = null;

		CSetInfo lastInfo;

		logger.debug("initWitness()");
		if (!witnessTable.exists()) {
			try {
				witnessTable.createNewFile();
				logger.debug("Table not exist, create connect table file!");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.info("Create connect table failed!");
			}
		}else{
			logger.debug("Connect table file exist!");
			FileReader fileReader = new FileReader(witnessTable);
			BufferedReader bufferReader = new BufferedReader(fileReader);
			String readLine;
			try {
				while((readLine = bufferReader.readLine()) != null)
				{
					readLine = readLine.trim();	//trim() 會將開頭跟結尾的空格省略
					String[] array = readLine.split(" ");
					logger.debug("array.length is = " + array.length);		//array.length 剛開始會等於1 ==> 需要過濾
					int fileExist = array.length;
					if (fileExist != 0 && fileExist != 1)
					{
						nodeId = Long.parseLong(array[0].split(":")[1]); //gluster_id :123 ==>line.split(" :")[1]) = 123
						logger.debug("nodeId : " + nodeId);
						ApId = Long.parseLong(array[1].split(":")[1]);
						logger.debug("ApId : " + ApId);
						master = array[2].split(":")[1];
						logger.debug("master :" + master);
						lockOwner = array[3].split(":")[1];
						logger.debug("lockOwner :" + lockOwner);
						lastInfo = new CSetInfo(Long.toString(ApId), Long.toString(nodeId), master, lockOwner);
						CSetInfo.setData(Long.toString(nodeId), lastInfo);	//類別函式可以用
					}
				}
				
			bufferReader.close();
			logger.debug("Connect table entry size : "+ nodeInfo.size() + ", lastInfo update already done!");
			} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
					// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void runServerThread()
	{
		try
		{
			synchronized(isRunning)		//20020513	lock
			{
				if (!isRunning)
				{
					globalServerSocket = new ServerSocket(this.port);	//建一個socket物件
					//20200601	這是測試用
//					globalUpDateTable = new CUpDateByHand();
//					globalUpDateTable.start();
					this.start();	//20200513	執行run
					
					isRunning = true;
				}
			}	//一次只會run一個socket thread
		} catch (IOException e) {
			logger.debug(e);
		}
	}
	
	@Override
	public void run()
	{
		Socket socket;
		try {
			while (true)
			{
				logger.debug("receiving...");
				socket = globalServerSocket.accept();	//20200513	監聽socket client端的請求
				logger.debug("accepted");
				WitnessReceiver receiver = new WitnessReceiver(socket);		//20200513	有client請求就建一個名為reciver的WitnessReceiver物件
				serverReceiver.add(receiver);	//20200513	可以看有幾個client
				
				receiver.start();	//20200514	WitnessRecevier.java run()
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		isRunning = false;
	}
}