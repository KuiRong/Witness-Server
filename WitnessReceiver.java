package java_server;

import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;

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
	
	private Socket socket;
	private byte[] header;
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
	
	public WitnessReceiver(Socket socket)
	{
		this.header = new byte[HEADER_LENGTH];	//20020513	長度48
		this.socket = socket;
		new ArrayList<Character>();
		try {
			this.socket.setSoTimeout(TIMEDOUT);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Socket getSocket() {
		return this.socket;
	}
	
	@Override
	public void run()
	{
		Command command;	//20200524	Command.java
		String magic;
		String testString;
		int count = 0;
		int total = 0;
		try {
			
			socketWriter = socket.getOutputStream();
			socketReader = socket.getInputStream();
			
			while(true)
			{
				count = socketRead(socketReader, total, HEADER_LENGTH - total);	//20200514	自己寫的method	//header這裡就會有值
				if (count == -2) {
					testString = new String(header);
					logger.info("count = -2 and header is "+ testString);
				}
				if (count >= 0)
					total = total + count;
				else
				{
					this.socket.close();
					logger.info("socket close");
					break;
				}		
				logger.debug("receive " + count + " bytes. (it shoud be 48 bytes)");
				
				if (total > 7)
				{
					magic = new String(header, 0, 8);		//20200514	從陣列header的0的位置起出長度8做為新的字串(magic)
					logger.debug(magic);
				}
				
				if (count == -1 || count == 0)
					break;
				
				if (total != HEADER_LENGTH)				//20200514	讀到完為止
					continue;

				magic = new String(header, 0, 8);
				
				if (!magic.equals(MAGIC))			//20200514 "WITNESS ";
				{
					logger.info("magic dismatch");
					logger.info(header);
					/*we needs handle remaining here*/
					continue;
				}
				else			//20200514	清成0
				{
					total = 0;
				}
				command = new Command(header);
				logger.debug("Entry Response");
				command.response(socketWriter);
			}
			
		} catch (SocketTimeoutException e) {
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	private int socketRead(InputStream socketReader, int offset, int length)
	{
		return this.socketRead(socketReader, header, offset, length);
	}
	
	private int socketRead(InputStream socketReader, byte[] buffer, int offset, int length)
	{	
		int count = 0;
		int readCount = 0;
		while (offset + readCount < length)
		{
			try
			{
				logger.debug("readCount is " + readCount + ", offset is " + offset + ",length is " + length);
				if (socketReader.available() <= 0)		//20200514	確保br2這個buffer有東西才read		回傳值-1等於斷線
				{
					logger.debug("count is " + count);
					count++;
					if (count == 5) {
						return -2;
					}
					Thread.sleep(1000);			//buffer不太可能沒東西然後等十秒
				}
				readCount = socketReader.read(buffer, offset, length);
			} catch (InterruptedException e) {
				continue;
			} catch (IOException e) {
				return -1;
			}
		}
		
		return readCount;
	}
}
