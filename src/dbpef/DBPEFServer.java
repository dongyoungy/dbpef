/*
 *  DBPEFServer.java
 *  
 *  Run DBPEF as a server in a distributed environment.
 *  
 *  This class extends DBPEF class as it shares some variables from DBPEF.
 *  
 *  DBPEFServer automatically finds its available DBPEFClient and sends its benchmark
 *  class and configuration file to them. It can then run the benchmark on 
 *  DBPEFClient machines simultaneously at the same time and gathers results from 
 *  each of DBPEFClient as the benchmark finishes.
 *  
 *  The code is built using Apple's Bonjour API. In order for DBPEFServer/DBPEFClient
 *  to work, a machine needs to have Bonjour service running on its system.
 *  
 *  @author Danny Dong Young Yoon
 */

package dbpef;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import com.apple.dnssd.BrowseListener;
import com.apple.dnssd.DNSSD;
import com.apple.dnssd.DNSSDRegistration;
import com.apple.dnssd.DNSSDService;
import com.apple.dnssd.RegisterListener;
import com.apple.dnssd.TXTRecord;

public class DBPEFServer extends DBPEF implements Runnable, RegisterListener, BrowseListener {
	
	/* Local variables */
	private static final String serviceName = "dbpef_server";
	private static final String clientName = "dbpef_client";
	private static final String STAND_BY = "stand-by";
	private static final String RUNNING = "running";
	private static final String QUIT = "quit";
	private DNSSDRegistration register;
	private ArrayList<DBPEFResult> results;
 	//private TXTRecord txtRecord;
	private ServerSocketChannel listeningChannel;
	private boolean timeSenderStop;
	private long startTime;
	private int listeningPort;
	private int clientCount;
	private int runningClientCount, steadyClientCount, finishedClientCount;
	
	/**
	 * Constructor
	 */
	public DBPEFServer() {
		super();
		try
		{
			writer = new PEFResultWriter("ServerResult");
			config = new Configuration();
			timeSenderStop = false;
			results = new ArrayList<DBPEFResult>();
			this.initBenchmark();
			//this.init(); // calls an initialise function
			System.out.println();
			logger.info("DBPEF Server is starting in a distributed mode...");
			
			listeningChannel = ServerSocketChannel.open();
			listeningChannel.socket().bind(new InetSocketAddress(0));
			listeningPort = listeningChannel.socket().getLocalPort();
			register = DNSSD.register(serviceName, serviceType, listeningPort, this);
			DNSSD.browse(serviceType, this);
			clientCount = 0;
			
			TXTRecord txtRecord = new TXTRecord();
			txtRecord.set("txtvers","1");
			txtRecord.set("command", STAND_BY);
			txtRecord.set("start_time", String.valueOf(Long.MAX_VALUE));
			
			register.getTXTRecord().update(0, txtRecord.getRawBytes(), 0);
			new Thread(new CommandReader()).start();
			new Thread(this).start();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/* Below are methods that are needed to be implemented by Bonjour API. */
	
	/**
	 * Automatically invoked when Bonjour operation fails.
	 */
	public void operationFailed(DNSSDService service, int errorCode)
    {
    	logger.log().warning("Operation Failed: " + errorCode);
    }
	/**
	 * Automatically invoked when a DBPEFServer service is successfully registered.
	 */
	public void serviceRegistered(DNSSDRegistration registration, int flags, String serviceName, String regType, String domain) {
    	
		logger.info("DBPEF Server registered. Server name: " + serviceName);
		
    }
	/**
	 * Automatically invoked when an available DBPEFClient is found on the network.
	 */
	public void serviceFound(DNSSDService browser, int flags, int ifIndex, 
			String name, String regType, String domain)
	{
		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException e)
		{
			logger.severe("Thread interrupted during serviceFound");
		}
		if (name.startsWith(clientName))
		{
			System.out.println();
			logger.info("New DBPEF Client has been found: " + name);
			clientCount++;
			logger.info("Currently available clients = " + clientCount);
			System.out.print("Type 'go' to start benchmark on all available clients / 'quit' to quit: ");
		}
	}
	/**
	 * Automatically invoked when a DBPEFClient becomes unavailable.
	 */
	public void serviceLost(DNSSDService browser, int flags, int ifIndex, 
				String name, String regType, String domain)
	{
		try
		{
			Thread.sleep(1000);
		}
		catch (InterruptedException e)
		{
			logger.severe("Thread interrupted during serviceLost");
		}
		if (name.startsWith(clientName))
		{
			System.out.println();
			logger.info("A DBPEF Client has been lost: " + name);
			clientCount--;
			logger.info("Currently available clients = " + clientCount);
			if (clientCount > 0) System.out.print("Type 'go' to start benchmark on all available clients / type 'quit' to quit: ");
			else System.out.print("Type 'quit' to quit: ");
		}
	}
	/* Bonjour API Methods ends */
	
	/**
	 * As this class implements Runnable, this run method getting invoked using 
	 * itself in order to listen to coming requests from DBPEFClients.
	 */
	public void run() {
		try
		{
			while (true)
			{
				SocketChannel sc = listeningChannel.accept();
				//System.out.println("!@#%@#!$%@!%");
				DataInputStream input = new DataInputStream(sc.socket().getInputStream());
				int status = input.readInt();
				//System.out.println(status);
				if (sc != null && status == CLIENT_NOT_READY) 
				{
					new Thread(this).start();
					new Thread(new BenchmarkSender(sc)).start();
				}
				if (sc != null && status == CLIENT_STEADY)
				{
					steadyClientCount--;
					if (steadyClientCount == 0) startTime = System.currentTimeMillis()+10000;
					/*
					if (runningClientCount == 0)
					{
						System.out.println("START!!!!!!");
						String startTime = String.valueOf(System.currentTimeMillis()+15000);
						TXTRecord txtRecord = new TXTRecord();
						txtRecord.set("start_time", startTime);
						register.getTXTRecord().update(0, txtRecord.getRawBytes(), 0);
					}*/
					new Thread(new StartTimeSender(sc)).start();
				}
				if (sc != null && status == CLIENT_GO)
				{
					runningClientCount--;
					new Thread(new ResultReceiver(sc)).start();
				}
				
			}
		}
		catch (Exception e)
		{
			
		}
	}
	
	public synchronized void printBenchResult(DBPEFResult newResult) {
		results.add(newResult);
		finishedClientCount--;
		int interval = newResult.getInterval();
		int numInterval = newResult.getThroughput().size();
		int numThroughput = newResult.getThroughput().get(0).length;
		String[] funcName = newResult.getFunctionName();
		long startTime = newResult.getStartTime();
		long[] rt = new long[numThroughput];
		long[] rt_sq = new long[numThroughput];
		long[] n = new long[numThroughput];
		
		if (finishedClientCount == 0)
		{
			logger.info("Clients finished their benchmark.");
			logger.info("Printing average throughput into a file...");
			int numClient = results.size();
			float[][] avgThroughput = new float[numInterval][numThroughput];
			float[] avgTotalThroughput = new float[numInterval];
			writer.startWriteResponse(startTime);
			for (int i=0;i<numClient;i++)
			{
				DBPEFResult result = results.get(i);
				ArrayList<float []> throughputList = result.getThroughput();
				ArrayList<Float> totalThroughputList = result.getTotalThroughput();
				for (int j=0;j<throughputList.size();j++)
				{
					float[] throughput = throughputList.get(j);
					float totalThroughput = totalThroughputList.get(j).floatValue();
					for (int k=0;k<throughput.length;k++)
					{
						avgThroughput[j][k] += throughput[k];
					}
					avgTotalThroughput[j] += totalThroughput;
				}
				
				long[] res = result.getResponseTime();
				for (int j=0;j<rt.length;j++)
				{
					rt[j] += res[j];
				}
				res = result.getResponseTimeSq();
				for (int j=0;j<rt_sq.length;j++)
				{
					rt_sq[j] += res[j];
				}
				res = result.getBenchComplated();
				for (int j=0;j<n.length;j++)
				{
					n[j] += res[j];
				}
			}
			
			for (int i=0;i<rt.length;i++)
			{
				float mean = (float)rt[i]/(float)n[i];
				double stddev = ((float)rt_sq[i]/(float)n[i] - (float)Math.pow((double)mean, 2.0));
				stddev = Math.sqrt(stddev);
				this.writer.writeResponseTimeResult(funcName[i], mean, stddev);
			}
			
			long rtTotal = 0;
			long rtSqTotal = 0;
			long nTotal = 0;
					
			for (int i=0;i<rt.length;i++)
			{
				rtTotal += rt[i];
				rtSqTotal += rt_sq[i];
				nTotal += n[i];
			}
			
			float mean = (float)rtTotal/(float)nTotal;
			double stddev = ((double)rtSqTotal/(double)nTotal - (double)(mean * mean));
			stddev = Math.sqrt(stddev);
			System.out.println(rt + " " + rt_sq + " " + n + " " + mean + " " + stddev);
			this.writer.writeResponseTimeResult("Total", mean, stddev);
						
			for (int j=0;j<numInterval;j++)
			{
				avgTotalThroughput[j] /= (float)numClient;
				for (int k=0;k<numThroughput;k++)
				{
					avgThroughput[j][k] /= (float)numClient;
				}
			}
			writer.startWriteAvgThroughput();
			writer.writeThroughputHeader(funcName);
			for (int j=0;j<numInterval;j++)
			{
				writer.writeThroughputResult(interval * (j+1), avgThroughput[j], avgTotalThroughput[j]);
			}
			writer.writeMeasureStartingTime(startTime);
			
			logger.fine("Benchmark has completed successfully.");
			System.exit(0);
		}
	}
			
	
	/* A private class inside DBPEFServer for getting command line inputs from a user.
	 * 
	 * @author Danny Dong Young Yoon
	 *
	 */
	private class CommandReader implements Runnable {
		public void run() {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			while (true)
			{
				try 
				{
					String input = br.readLine();
					if (input != null) input = input.toLowerCase();
					if (input.equalsIgnoreCase(GO_SIGNAL))
					{
						TXTRecord txtRecord = new TXTRecord();
						txtRecord.set("command", GO_SIGNAL);
						register.getTXTRecord().update(0, txtRecord.getRawBytes(), 0);
						runningClientCount = clientCount;
						steadyClientCount = clientCount;
						finishedClientCount = clientCount;
						logger.info("GO signal sent to all available clients.");
						Thread.sleep(5000);
						txtRecord.set("command", RUNNING);
						logger.info("Waiting for every client to finish its benchmark...");
						register.getTXTRecord().update(0, txtRecord.getRawBytes(), 0);
					}
					if (input.equalsIgnoreCase(QUIT))
					{
						System.out.println("Closing down...");
						tearDown();
						System.exit(0);
					}
				}
				catch (Exception e)
				{
					
				}
			}
		}
	}
	
	private class StartTimeSender implements Runnable {
		private SocketChannel sc;
		public StartTimeSender(SocketChannel s) {
			sc = s;
		}
		public void run() {
			DataOutputStream dataOut = null;
			try
			{
				dataOut = new DataOutputStream(sc.socket().getOutputStream());
				//objIn = new ObjectInputStream(sc.socket().getInputStream());
			}
			catch (IOException e)
			{
				logger.severe("IOException raised in ResultReceiver.");
				e.printStackTrace();
				System.exit(-1);
			}
			
			while (!timeSenderStop)
			{
				try
				{
					if (steadyClientCount == 0) 
					{
						dataOut.writeLong(startTime);
					}
				}
				catch(Exception e)
				{
					
				}
			}
		}
	}
	
	/* A private class inside DBPEFServer for getting results from DBPEFClients
	 * 
	 * @author Danny Dong Young Yoon
	 *
	 */
	private class ResultReceiver implements Runnable {
		
		private SocketChannel sc;
		public ResultReceiver(SocketChannel s) {
			sc = s;
		}
		
		public void run() {
			
			DataInputStream dataIn = null;
			//ObjectInputStream objIn = null;
			try
			{
				while (runningClientCount > 0)
				{
				}
				timeSenderStop = true;
				ObjectInputStream objIn = new ObjectInputStream(sc.socket().getInputStream());
				DBPEFResult result = (DBPEFResult)objIn.readObject();
				printBenchResult(result);
				/*
				ArrayList<long []> throughput;
				ArrayList<Long> totalThroughput;
				throughput = result.getThroughput();
				totalThroughput = result.getTotalThroughput();
				System.out.println(throughput.size());
				for (int i=0;i<throughput.size();i++)
				{
					System.out.printf("%d", result.getInterval()*(i+1));
					long[] current = throughput.get(i);
					for (int j=0;j<current.length;j++)
					{
						System.out.printf(",%d",current[j]);
					}
					System.out.printf(",%d%n", ((Long)totalThroughput.get(i)).longValue());
				}*/
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
			
		}
	}
	
	/* A private class inside DBPEFServer for sending benchmark class file to DBPEFClients.
	 * 
	 * @author Danny Dong Young Yoon
	 *
	 */
	private class BenchmarkSender implements Runnable {
		
		private SocketChannel sc;
    	public BenchmarkSender(SocketChannel s) {
    		sc = s;
    	}
    	public void run() {
    		config = new Configuration();
    		try
    		{
    			ObjectOutputStream objOut = new ObjectOutputStream(sc.socket().getOutputStream());
	    		objOut.writeObject(config.getProperties());
				objOut.flush();
				
				System.out.println();
				logger.info("Sending benchmark configuration to a DBPEF client...");
		
				
				// sendfile
				ClassLoader loader = ClassLoader.getSystemClassLoader();
				
			    File myFile = new File(loader.getResource(config.getProp("filename")+".class").getPath());
			    byte [] mybytearray  = new byte [(int)myFile.length()];
			    FileInputStream fis = new FileInputStream(myFile);
			    BufferedInputStream bis = new BufferedInputStream(fis);
			    bis.read(mybytearray,0,mybytearray.length);
			    OutputStream os = sc.socket().getOutputStream();
			    logger.info("Sending benchmark file to a DBPEF client...");
			    os.write(mybytearray,0,mybytearray.length);
			    os.flush();
			    os.close();
			    logger.info("Benchmark files were successfully sent to the DBPEF client.");
    		}
    		catch (Exception e)
    		{
    			e.printStackTrace();
    		}
    	}
	}
}
