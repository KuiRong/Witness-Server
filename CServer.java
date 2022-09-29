package java_server;

// import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;


public class CServer extends Thread{

	private int port;
	private Boolean isRunning = false;
//	private ServerSocket globalServerSocket;
//	private  Future<AsynchronousSocketChannel> globalServerSocket;
	private  AsynchronousServerSocketChannel globalServerSocket;
	private  AsynchronousSocketChannel client = null;
//	private CUpDateByHand globalUpDateTable;	//for change connect table by user
	private HashSet<WitnessReceiver> serverReceiver;
	// private static Logger logger = Logger.getLogger(CServer.class);  

	public static long nodeOneSendCount = 0;
	public static long nodeTwoSendCount = 0;
	
	static HashMap<String, CSetInfo> nodeInfo = new HashMap<String, CSetInfo>();
	static HashMap<String, Integer> applianceCount = new HashMap<String, Integer>();
	
	static boolean judgeDone = false;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CServer server = new CServer(8000);
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
		this.serverReceiver = new HashSet<WitnessReceiver>();
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

		// logger.debug("initWitness()");
		if (!witnessTable.exists()) {
			try {
				witnessTable.createNewFile();
				// logger.debug("Table not exist, create connect table file!");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				// logger.info("Create connect table failed!");
			}
		}else{
			// logger.debug("Connect table file exist!");
			FileReader fileReader = new FileReader(witnessTable);
			BufferedReader bufferReader = new BufferedReader(fileReader);
			String readLine;
			try {
				while((readLine = bufferReader.readLine()) != null)
				{
					readLine = readLine.trim();
					String[] array = readLine.split(" ");
					// logger.debug("array.length is = " + array.length);
					int fileExist = array.length;
					if (fileExist != 0 && fileExist != 1)
					{
						nodeId = Long.parseLong(array[0].split(":")[1]); //gluster_id :123 ==>line.split(" :")[1]) = 123
						// logger.debug("nodeId : " + nodeId);
						ApId = Long.parseLong(array[1].split(":")[1]);
						// logger.debug("ApId : " + ApId);
						master = array[2].split(":")[1];
						// logger.debug("master :" + master);
						lockOwner = array[3].split(":")[1];
						// logger.debug("lockOwner :" + lockOwner);
						lastInfo = new CSetInfo(Long.toString(ApId), Long.toString(nodeId), master, lockOwner);
						CSetInfo.setData(Long.toString(nodeId), lastInfo);
					}
				}
				
			bufferReader.close();
			// logger.debug("Connect table entry size : "+ nodeInfo.size() + ", lastInfo update already done!");
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
//					globalServerSocket = new ServerSocket(this.port);
					globalServerSocket = AsynchronousServerSocketChannel.open();
					globalServerSocket.bind(new InetSocketAddress("172.27.12.89", 8000));
//					globalServerSocket.setSoTimeout(15000);
					//20200601	�o�O���ե�
//					globalUpDateTable = new CUpDateByHand();
//					globalUpDateTable.start();
					this.start();
					
					isRunning = true;
				}
			}
		} catch (IOException e) {
			// logger.debug(e);
		}
	}
	
	@Override
	public synchronized void run()
	{
		Socket socket;
		ByteBuffer buffer = ByteBuffer.allocate(48);
		try {
			while (true)
			{
				// logger.debug("receiving...");
		        Future<AsynchronousSocketChannel> acceptCon = globalServerSocket.accept();
//				socket = globalServerSocket.accept();
				// logger.debug("accepted");
				client = acceptCon.get();
				WitnessReceiver receiver = new WitnessReceiver(client);
				serverReceiver.add(receiver);
				// logger.info("serverReceiver.keyset() is " + serverReceiver.toString());
				receiver.start();
			}	
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		isRunning = false;
	}
}