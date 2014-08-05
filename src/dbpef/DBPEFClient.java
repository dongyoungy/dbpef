/*
 *  DBPEFClient.java
 *  
 *  Run DBPEF as a client in a distributed environment.
 *  
 *  This class extends DBPEF class as it shares variables with DBPEF and uses its
 *  runSingleMode method to run a benchmark.
 *  
 *  DBPEFClient gets the benchmark class and its configuration from DBPEFServer
 *  and is responsible for running them accordingly and return results 
 *  to DBPEFServer.
 *  
 *  The code is built using Apple's Bonjour API. In order for DBPEFServer/DBPEFClient
 *  to work, a machine needs to have Bonjour service running on its system. 
 *   
 *  @author Danny Dong Young Yoon
 */

package dbpef;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ConnectException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Properties;
import com.apple.dnssd.DNSSD;
import com.apple.dnssd.DNSSDRegistration;
import com.apple.dnssd.DNSSDService;
import com.apple.dnssd.QueryListener;
import com.apple.dnssd.RegisterListener;
import com.apple.dnssd.ResolveListener;
import com.apple.dnssd.TXTRecord;

public class DBPEFClient extends DBPEF implements RegisterListener, ResolveListener, QueryListener {
	
	/* Constants */
	private static final String serviceName = "dbpef_client";
	private static final String serverName = "dbpef_server";
	
	/* Local variables */
	private DNSSDService commandMonitor; // monitors DBPEFServer of its change of command.
	private SocketChannel sc;
	private SocketChannel resultSc, timeReceiverSc;
	private InetSocketAddress resultSocketAddress;
	private ServerSocketChannel listeningChannel;
	private int listeningPort;
	private int status;
	private long startTime = Long.MAX_VALUE;
	
	/**
	 * Constructor
	 */
	public DBPEFClient() {
		super();
		try
		{
			logger.info("DBPEF Client is starting in a distributed mode...");
			
			System.out.println();
			logger.info("Waiting for DBPEF server to be available...");
			DNSSDService r = DNSSD.resolve(0, DNSSD.ALL_INTERFACES, serverName, serviceType, serviceDomain, this);
			listeningChannel = ServerSocketChannel.open();
	    	listeningChannel.socket().bind(new InetSocketAddress(0));
	    	listeningPort = listeningChannel.socket().getLocalPort();
	    	DNSSD.register(serviceName, serviceType, listeningPort, this);
	    	//DNSSDService b = DNSSD.browse("_dbpef._tcp", this);
	    	status = CLIENT_NOT_READY;
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
    	logger.warning("Operation Failed: " + errorCode);
    }
	/**
	 * Automatically invoked when a DBPEFClient service is successfully registered.
	 */
	public void serviceRegistered(DNSSDRegistration registration, int flags, String serviceName, String regType, String domain) {
		
		logger.info("DBPEF Client registered. Client name: " + serviceName);
		
    }
	
	/**
	 * Automatically invoked when a detected DBPEFServer changes its command.
	 */
	public void queryAnswered(DNSSDService query, int flags, int ifIndex, String fullName,
			int rrtype, int rrclass, byte[] rdata, int ttl) {
		
		if ((flags & 2) != 0)
		{
			TXTRecord txtRecord = new TXTRecord(rdata);
			for (int i=0;i<txtRecord.size();i++)
			{
				String key = txtRecord.getKey(i);
				String value = null;
				if (key != null) key = key.toLowerCase();
				if (key.equals("command")) 
				{
					value = txtRecord.getValueAsString(i);
					if (value != null && value.equals(GO_SIGNAL)) runDistMode();
				}
				if (key.equals("start_time")) 
				{
					value = txtRecord.getValueAsString(i);
					startTime = Long.parseLong(value);
				}
			}
		}
	}
	/**
	 * Automatically invoked when DBPEFClient resolves a DBPEFServer.
	 */
	public void serviceResolved(DNSSDService resolver, int flags, int ifIndex, 
			String fullName, String hostName, int port, TXTRecord txtRecord) {

		logger.info("DBPEF Server resolved: " + hostName);

		try
		{
			InetSocketAddress socketAddress = new InetSocketAddress(hostName, port);
			resultSocketAddress = new InetSocketAddress(hostName, port);
			sc = SocketChannel.open(socketAddress);
			DataOutputStream out = new DataOutputStream(sc.socket().getOutputStream());
			out.writeInt(status);
			out.flush();
			Thread receiver = null;
			if (status == CLIENT_NOT_READY)
			{
				receiver = new Thread(new BenchmarkReceiver(sc));
				receiver.start();
				logger.info("Receiving benchmark file and configuraion from DBPEF server...");
				while (receiver.isAlive()) 
				{
				}
				status = CLIENT_READY;
				resolver.stop();
				Thread.sleep(1);
				DNSSD.resolve(0, DNSSD.ALL_INTERFACES, serverName, serviceType, serviceDomain, this);
				logger.info("Client received benchmark files and now ready to run the benchmark.");
			}
			else if (status == CLIENT_READY)
			{
				/*
				DataOutputStream dataOut = new DataOutputStream(timeReceiverSc.socket().getOutputStream());
				dataOut.writeInt(CLIENT_GO);
				dataOut.flush();*/
				commandMonitor = DNSSD.queryRecord(0, ifIndex, fullName, 16, 1, this);
				resolver.stop();
				Thread.sleep(1);
			}
		}
		catch (ConnectException e)
		{
			logger.info("Server is out of date");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Run benchmark in distributed mode and return results to the DBPEFServer.
	 */
	private void runDistMode() {
		
		logger.info("Recieved GO signal from DBPEF Server.");
		System.out.println();
		this.initBenchmark();
		
		boolean steadyState = false;
		boolean measureStart = false;
		boolean startTimeReceived = false;
		boolean resultChannelOpen = false;
		float[] currentThroughput = new float[numBenchFunction];
		float totalThroughput = 0;
		float lastTotalThroughput = 0;
		long benchStart = 0;
		long[] current = new long[numBenchFunction];
		long[] last = new long[numBenchFunction];
		int spinCycle = 0;
		int intervalCycle = 0;
		int numInterval = 0;
		long measureStartTime = 0;
		benchStart = System.currentTimeMillis();
		writer.startWriteThroughput(benchStart);
		DBPEFResult result = new DBPEFResult(interval, numBenchFunction);
		ThreadGroup runners = new ThreadGroup("PEFRunners");
		PEFRunner runner[] = new PEFRunner[MPL];
		writer.writeThroughputHeader(funcName);
		result.setFunctionName(funcName);
		
		System.out.println();
		logger.info("DBPEF has started in a distributed mode...");
		System.out.println();
		if (userModel.toLowerCase().equals("mix")) logger.info("User model : Transaction mix");
		else logger.info("User model : Single transaction (" + singleFunc + ")");
		if (thinkTime > 0) logger.info("System type: Open system (Think time = " + thinkTime + " ms)" );
		else logger.info("System type: Closed system");
		
		
		try
		{
			for (int i=0;i<MPL;i++)
			{
				runner[i] = new PEFRunner(runners, "PEFRunner_" + (i + 1), benchClass.newInstance(),
						funcList, ratio, duration, thinkTime, funcName, resultPerThread);
			}
		}
		catch (Exception e)
		{
			logger.severe("Exception raised during threads creation");
			e.printStackTrace();
		}
		
		for (int i=0;i<MPL;i++)
		{
			runner[i].start();
		}
		
		System.out.print("Warm-up stage..");
		while (runners.activeCount() > 0)
		{
			try
			{
				Thread.sleep(1000);
				intervalCycle++;
				if (intervalCycle >= interval)
				{
					totalThroughput = 0;
					
					for (int i=0;i<current.length;i++)
					{
						current[i] = 0;
					}
					for (int i=0;i<numBenchFunction;i++)
					{
						for (int j=0;j<MPL;j++)
						{
							current[i] += runner[j].monitor.getBenchCompleted(i);
						}
						currentThroughput[i] = current[i] - last[i];
						currentThroughput[i] /= (float)interval;
						last[i] = current[i];
						totalThroughput += currentThroughput[i];
					}
					if (lastTotalThroughput > totalThroughput && !steadyState)
					{
						steadyState = true;
						timeReceiverSc = SocketChannel.open(resultSocketAddress);
						DataOutputStream out = new DataOutputStream(timeReceiverSc.socket().getOutputStream());
						out.writeInt(CLIENT_STEADY);
						out.flush();
						status = CLIENT_STEADY;
						System.out.println();
						logger.info("Steady-state reached: Waiting for server to start measurement...");
						System.out.println();
					}
					lastTotalThroughput = totalThroughput;
					if (resultPerThread)
					{
						for (int i=0;i<MPL;i++)
						{
							runner[i].writeThroughput(interval, numInterval);
						}
					}
					writer.writeThroughputResult((numInterval + 1) * interval, currentThroughput, totalThroughput);
					result.add(currentThroughput);
					result.addTotal(totalThroughput);
					numInterval++;
					intervalCycle = 0;
				}
				
				if (!steadyState)
				{
					System.out.print(".");
				}
				else
				{
					if (!startTimeReceived)
					{
						DataInputStream dataIn = new DataInputStream(timeReceiverSc.socket().getInputStream());
						startTime = dataIn.readLong();
						startTimeReceived = true;
					}
					else
					{
						if (!resultChannelOpen) 
						{
							resultSc = SocketChannel.open(resultSocketAddress);
							resultChannelOpen = true;
							DataOutputStream dataOut = new DataOutputStream(resultSc.socket().getOutputStream());
							dataOut.writeInt(CLIENT_GO);
						}
					}
					
					if (!measureStart && System.currentTimeMillis() >= startTime)
					{
						System.out.println("DBPEF Server has confirmed that SUT is in steady-state.");
						writer.startWriteResponse(startTime);
						PEFMonitor.startMeasure(startTime);
						measureStartTime = (startTime - benchStart) / 1000;
						for (int k=0;k<MPL;k++)
						{
							runner[k].startMeasure();
						}
						for (int k=0;k<numBenchFunction;k++)
						{
							last[k] = 0;
						}
						measureStart = true;
					}
					if (measureStart)
					{
						System.out.print("DBPEF is now running... " + spins[spinCycle] + "\b\r");
						spinCycle = (spinCycle + 1) & 3;
					}
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
		/*
		boolean steadyState = false;
		boolean measureStart = false;
		long currentThroughput = 0; 
		long lastThroughput = 0;
		long current = 0;
		long last = 0;
		int spinCycle = 0;
		System.out.print("Warm-up stage..");
		while (runners.activeCount() > 0)
		{
			if (!steadyState) 
			{
				try
				{
					Thread.sleep(interval * 1000);
					current = 0;
					for (int i=0;i<MPL;i++)
					{
						current += runner[i].monitor.getTotalBenchCompleted();
					}
					System.out.print(".");
					currentThroughput = current - last;
					if (currentThroughput < lastThroughput) 
					{
						System.out.println();
						logger.info("Steady-state reached: Waiting for server to start measurement...");
						System.out.println();
						DataOutputStream out = new DataOutputStream(resultSc.socket().getOutputStream());
						out.writeInt(CLIENT_STEADY);
						out.flush();
						status = CLIENT_STEADY;
						steadyState = true;
					}
					lastThroughput = currentThroughput;
					last = current;
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				if (!measureStart)
				{
					if (System.currentTimeMillis() >= startTime) 
					{
						PEFMonitor.startMeasure(startTime);
						for (int i=0;i<MPL;i++) 
						{
							runner[i].startMeasure();
						}
						measureStart = true;
					}
				}
				try
				{
					Thread.sleep(250);
					System.out.print("DBPEF is now running... " + spins[spinCycle] + "\b\r");
					spinCycle = (spinCycle + 1) & 3;
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		ArrayList<float []> throughput;
		ArrayList<Float> totalThroughput1;
		throughput = result.getThroughput();
		totalThroughput1 = result.getTotalThroughput();
		System.out.println(throughput.size());
		for (int i=0;i<throughput.size();i++)
		{
			System.out.printf("%d", result.getInterval()*(i+1));
			long[] current1 = throughput.get(i);
			for (int j=0;j<current1.length;j++)
			{
				System.out.printf(",%d",current1[j]);
			}
			System.out.printf(",%d%n", ((Long)totalThroughput1.get(i)).longValue());
		}*/
		
		writer.writeMeasureStartingTime(measureStartTime);
		result.setStartTime(measureStartTime);
		result.setBenchCompleted(benchCompleted);
		result.setBenchFailed(benchFailed);
		result.setResponseTime(responseTime);
		result.setResponseTimeSq(responseTimeSq);
		
		for (int i=0;i<funcName.length;i++)
		{
			long rt = responseTime[i];
			long rt_sq = responseTimeSq[i];
			long n = benchCompleted[i];
			float mean = (float)rt/(float)n;
			double stddev = ((float)rt_sq/(float)n - (float)Math.pow((double)mean, 2.0));
			stddev = Math.sqrt(stddev);
			this.writer.writeResponseTimeResult(funcName[i], mean, stddev);
		}
		
		long totalBenchCompleted = 0;
		long totalBenchFailed = 0;
		long totalResponseTime = 0;
		long totalResponseTimeSq = 0;
		
		for (int i=0;i<numBenchFunction;i++)
		{
			totalBenchCompleted += benchCompleted[i];
			totalBenchFailed += benchFailed[i];
			totalResponseTime += responseTime[i];
			totalResponseTimeSq += responseTimeSq[i];
		}
		
		long rt = totalResponseTime;
		long rt_sq = totalResponseTimeSq;
		long n = totalBenchCompleted;
		float mean = (float)rt/(float)n;
		double stddev = ((double)rt_sq/(double)n - (double)(mean * mean));
		stddev = Math.sqrt(stddev);
		System.out.println(rt + " " + rt_sq + " " + n + " " + mean + " " + stddev);
		this.writer.writeResponseTimeResult("Total", mean, stddev);
		
		benchCompletionTime = System.currentTimeMillis();
		benchStartTime = startTime;
		System.out.println();
		
		logger.info("DBPEF completed.");
		try 
		{
			/*
			DataOutputStream out = new DataOutputStream(resultSc.socket().getOutputStream());
			out.writeInt(3);
			out.flush();*/
			ObjectOutputStream objOut = new ObjectOutputStream(resultSc.socket().getOutputStream());
			objOut.writeObject(result);
			objOut.flush();
			Thread.sleep(1);
			this.printStat();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		try
		{
			
			
		}
		catch (Exception e)
		{
			
		}
		System.exit(0);
		
	}
	
	/*
	 * A private class for receiving a benchmark class from DBPEFServer.
	 */
	private class BenchmarkReceiver implements Runnable {
    	
    	private SocketChannel sc;
    	public BenchmarkReceiver(SocketChannel s) {
    		sc = s;
    	}
    	
    	public void run() {
    		try
    		{
   				ObjectInputStream objIn = new ObjectInputStream(sc.socket().getInputStream());
	   			Properties prop = (Properties)objIn.readObject();
	   			config = new Configuration(prop);
	    		
            	int filesize=7777777; // filesize temporary hardcoded

			    int bytesRead;
    			int current = 0;

			 	byte [] mybytearray  = new byte [filesize];
			    InputStream is = sc.socket().getInputStream();
			    FileOutputStream fos = new FileOutputStream(config.getProp("filename")+".class");
			    BufferedOutputStream bos = new BufferedOutputStream(fos);
			    bytesRead = is.read(mybytearray,0,mybytearray.length);
			    current = bytesRead;
			
			    do {
			       bytesRead =
			          is.read(mybytearray, current, (mybytearray.length-current));
			       if(bytesRead >= 0) current += bytesRead;
			    } while(bytesRead > -1);
			
			    bos.write(mybytearray, 0 , current);
			    bos.flush();
			    bos.close();
			    is.close();

    		}	
    		catch (Exception e)
    		{
    			e.printStackTrace();
    		}
	   	}
    	
    }
	
}
