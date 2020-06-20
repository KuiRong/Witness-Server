package java_server;

import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class WitnessReceiver extends Thread {
	
	private static Logger logger = Logger.getLogger(WitnessReceiver.class);
	public static final String MAGIC = "WITNESS ";
	public static final String WITNESS_MAGIC = "WTSERVER"; 
	public static final int HEADER_LENGTH = 48;
	
	private static final int TIMEDOUT = 10 * 1000; // 10 secs for 3 heartbeats miss
	
//	private Socket socket;
	private AsynchronousSocketChannel clientsocket = null;
//	private byte[] header;
	private ByteBuffer header;
	private byte[] buffer;
	public byte[] data;
	
	private OutputStream socketWriter;
	private InputStream socketReader;
		
	public int crc;
	public short op;
	public short length;
	public Long gluster_id;
	public Long nodeId;
	public Long generation;
	
//	private Boolean isDataVaild;
	//////////////////////////////////////////////////////////////////////////////////
	
	public WitnessReceiver(AsynchronousSocketChannel clientsocket)
//	public WitnessReceiver(Socket socket)
	{
		this.header = ByteBuffer.allocate(48);	//20020513	長度48
		this.clientsocket = clientsocket;
	}

//	public Socket getSocket() {
//		return this.socket;
//	}

	public AsynchronousSocketChannel getSocket() {
		return this.clientsocket;
	}
	
	@Override
	public synchronized void run()
	{
		Command command;	//20200524	Command.java
		String magic;
//		String testString;
		AsynchronousSocketChannel client = null;
		int count = 0;
		int total = 0;
		try {
//			socketWriter = socket.getOutputStream();
//			socketReader = socket.getInputStream();
			client = this.clientsocket;
//			ByteBuffer buffer = ByteBuffer.allocate(48);
//			header = buffer;
			while(true)
			{
				count = 0;
				if ((client!= null) && (client.isOpen())) {
					logger.info("[CURRENT THREAD] is " + Thread.currentThread().getName());
//					count = socketRead(client, total, HEADER_LENGTH - total);	//20200514	自己寫的method	//header這裡就會有值
					count = socketRead(client);
					this.buffer = new byte[WitnessReceiver.HEADER_LENGTH];
//					WitnessReceiver.header.get(this.buffer, 0 , WitnessReceiver.HEADER_LENGTH);
					logger.info("count is : " + count );
					if (count >= 0) {
						total = total + count;
						try {
//							this.header.get(this.buffer, 0 , WitnessReceiver.HEADER_LENGTH);
							this.header.get(this.buffer);
							logger.info("Received from client: " + new String(this.buffer).trim());
						} catch (Exception e) {
							logger.info("e is : " + e );
							command = new Command(this.buffer);
							command.unknownCommand(client);
						}
					}
					else
					{
//						this.socket.close();
//						logger.info("socket close");
						logger.info("[socket close] count is " + count);
						break;
					}
					logger.info("[CURRENT THREAD] is " + Thread.currentThread().getName() +
							" receive " + count + " bytes. (it shoud be 48 bytes)");
					
					if (total > 7)
					{
						magic = new String(this.buffer, 0, 8);		//20200514	從陣列header的0的位置起出長度8做為新的字串(magic)
						logger.debug(magic);
					}
					
					if (count == -1 || count == 0)
						break;
					
					if (total != HEADER_LENGTH)				//20200514	讀到完為止
						continue;

					magic = new String(this.buffer, 0, 8);
					if (!magic.equals(MAGIC))			//20200514 "WITNESS ";
					{
						logger.info("magic dismatch");
						logger.info("magic is " + magic);
						logger.info(header);
						/*we needs handle remaining here*/
						command = new Command(this.buffer);
						command.unknownCommand(client);
//						continue;
					}
					else			//20200514	清成0
					{
						total = 0;
					}
					total = 0;
					command = new Command(this.buffer);
					logger.debug("Entry Response");
//					command.response(socketWriter);
					command.response(client);
				}
			}
//				count = socketRead(socketReader, total, HEADER_LENGTH - total);	//20200514	自己寫的method	//header這裡就會有值
//				count = socketRead(client, total, HEADER_LENGTH - total);	//20200514	自己寫的method	//header這裡就會有值
//				if (count == -2) {
//					testString = new String(header);
//					logger.info("count = -2 and header is "+ testString);
//				}
//				if (count >= 0)
//					total = total + count;
//				else
//				{
//					this.socket.close();
//					logger.info("socket close");
//					logger.info("[socket close] count is " + count);
//					break;
//				}		
//				logger.debug("receive " + count + " bytes. (it shoud be 48 bytes)");
//				
//				if (total > 7)
//				{
//					magic = new String(header, 0, 8);		//20200514	從陣列header的0的位置起出長度8做為新的字串(magic)
//					logger.debug(magic);
//				}
//				
//				if (count == -1 || count == 0)
//					break;
//				
//				if (total != HEADER_LENGTH)				//20200514	讀到完為止
//					continue;
//
//				magic = new String(header, 0, 8);
//				
//				if (!magic.equals(MAGIC))			//20200514 "WITNESS ";
//				{
//					logger.info("magic dismatch");
//					logger.info(header);
//					/*we needs handle remaining here*/
//					continue;
//				}
//				else			//20200514	清成0
//				{
//					total = 0;
//				}
//				command = new Command(header);
//				logger.debug("Entry Response");
//				command.response(socketWriter);
//			}
			
		} catch (SocketTimeoutException e) {
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private synchronized int socketRead(AsynchronousSocketChannel socketclient) throws InterruptedException, IOException
	{
		return this.socketRead(socketclient, this.header);
	}
	
	private synchronized int socketRead(AsynchronousSocketChannel socketclient, ByteBuffer buffer) throws InterruptedException, IOException
	{
//		int count = 0;
		int readCount = 0;
//		while (offset + readCount < length)
//		{
//		buffer.flip();
		buffer.clear();
		Future<Integer> readval = socketclient.read(buffer);
//		buffer.flip();
		try {
			readCount = readval.get();
			logger.info("[CURRENT THREAD] is " + Thread.currentThread().getName() + 
					" readCount: " + readCount);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("Received from client: " + new String(buffer.array()).trim());
//			buffer.flip();
//			readCount = socketReader.read(buffer, offset, length);
//			try {
//				readCount = readval.get();
//			} catch (InterruptedException | ExecutionException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		return readCount;
	}
	
//	private synchronized int socketRead(InputStream socketReader, int offset, int length) throws InterruptedException, IOException
//	{
//		return this.socketRead(socketReader, header, offset, length);
//	}
	
	private int socketRead(InputStream socketReader, byte[] buffer, int offset, int length) throws InterruptedException, IOException
	{	
		int count = 0;
		int readCount = 0;
		while (offset + readCount < length)
		{
//			try
//			{
			logger.info("[socketRead]readCount is " + readCount + ", offset is " + offset + ",length is " + length);
			if (socketReader.available() <= 0)		//20200514	確保br2這個buffer有東西才read		回傳值-1等於斷線
			{
				logger.info("[socketRead]count is " + count);
				count++;
				if (count == 5) {
					return -2;
				}
				Thread.sleep(1000);			//buffer不太可能沒東西然後等十秒
			}
			readCount = socketReader.read(buffer, offset, length);
//			} catch (InterruptedException e) {
//				continue;
//			} catch (IOException e) {
//				return -1;
//			}
		}
		
		return readCount;
	}
}
