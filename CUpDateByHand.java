package java_server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CUpDateByHand extends Thread{
//public class CUpDateByHand{
	
	public CUpDateByHand() {
		System.out.print("Create update table object!!");
	}
	@Override
	public void run()
	{
		while(true) {
			try {
				sleep(5000);
			} catch (InterruptedException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			File witnessTable = new File("./gluster.txt");
			Long nodeId = null;
			Long ApId = null;
			String master = null;
			String lockOwner = null;

			CSetInfo lastInfo;

			System.out.println("UpDateByhandle!");
			if (!witnessTable.exists()) {
				try {
					witnessTable.createNewFile();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				System.out.println("gluster.txt exist! ");
				FileReader fileReader = null;
				try {
					fileReader = new FileReader(witnessTable);
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				BufferedReader bufferReader = new BufferedReader(fileReader);
				String readLine;
				try {
					while((readLine = bufferReader.readLine()) != null)
					{
						readLine = readLine.trim();
						String[] array = readLine.split(" ");

						int fileExist = array.length;
						if (fileExist != 0 && fileExist != 1)
						{
							nodeId = Long.parseLong(array[0].split(":")[1]); //gluster_id :123 ==>line.split(" :")[1]) = 123
//							System.out.println("nodeId : " + nodeId);
							ApId = Long.parseLong(array[1].split(":")[1]);
//							System.out.println("ApId : " + ApId);
							master = array[2].split(":")[1];
//							System.out.println("master :" + master);
							lockOwner = array[3].split(":")[1];
//							System.out.println("lockOwner :" + lockOwner);
							lastInfo = new CSetInfo(Long.toString(ApId), Long.toString(nodeId), master, lockOwner, "test");
							CSetInfo.setData(Long.toString(nodeId), lastInfo);
						}
					}
				
					bufferReader.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// 	TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}	
		}
	}
}