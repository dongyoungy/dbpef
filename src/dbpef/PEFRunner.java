/*
 *  PEFRunner.java
 *  
 *  Runnable object which invokes benchmark methods in a benchmark class according
 *  to the transaction mix given (or invokes just a single method) and records its
 *  response time to DBPEF class and a text file.
 *  
 *  @author Danny Dong Young Yoon
 */

package dbpef;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Random;

public class PEFRunner extends Thread {
	
	/* Local variables */
	private PEFLogger logger;
	public PEFMonitor monitor;
	public PEFResultWriter writer;
	private ArrayList<Method> functionList;
	private Object benchObj;
	private double[] ratio;
	private String[] funcName;
	private int duration;
	private long thinkTime;
	private boolean resultPerThread;
	private long[] current;
	private long[] last;
	private float[] currentThroughput;
	private float totalThroughput;
	
	
	/**
	 * Constructor
	 * @param group current group of a PEFRunner thread.
	 * @param threadName the name of a thread.
	 * @param obj benchmark object.
	 * @param list list of benchmark methods.
	 * @param ratio an array of double representing transaction mix.
	 * @param duration duration of benchmark in seconds.
	 * @param thinkTime think time of simulated user in milliseconds.
	 */
	public PEFRunner(ThreadGroup group, String threadName, Object obj, ArrayList<Method> list,
			double[] ratio, int duration, long thinkTime, String[] funcName, boolean resultPerThread) {
		
		super(group, threadName);
		benchObj = obj;
    	functionList = list;
    	this.ratio = ratio;
    	this.duration = duration;
    	this.thinkTime = thinkTime;
    	this.funcName = funcName;
    	this.resultPerThread = resultPerThread;
    	current = new long[list.size()];
    	last = new long[list.size()];
    	currentThroughput = new float[list.size()];
		logger = PEFLogger.INSTANCE;
		monitor = new PEFMonitor(functionList.size(), threadName);
		writer = new PEFResultWriter(threadName);
		if (resultPerThread) 
		{
			writer.startWriteThroughput(System.currentTimeMillis());
			writer.writeThroughputHeader(funcName);
		}
	}
	
	/**
	 * Signals its PEFMonitor that steady-state has been reached and start
	 * measuring performance.
	 */
	
	public void startMeasure() {
		monitor.init();
		for (int i=0;i<functionList.size();i++)
		{
			last[i] = 0;
		}
		
	}
	
	public void writeThroughput(int interval, int numInterval) {
		
		totalThroughput = 0;
		for (int i=0;i<functionList.size();i++)
		{
			current[i] = this.monitor.getBenchCompleted(i);
			currentThroughput[i] = current[i] - last[i];
			currentThroughput[i] /= (float)interval;
			last[i] = current[i];
			totalThroughput += currentThroughput[i];
		}
		writer.writeThroughputResult((numInterval + 1) * interval, currentThroughput, totalThroughput);
		//return currentThroughput;
	}
	
	/**
	 * Invokes benchmark methods continuously according to the rules specified in 
	 * a configuration file.
	 */
	public void run(){
		Boolean completed; 
		try
		{
			logger.log().info("Thread: " + 
					Thread.currentThread().getName() + " has been started.");
			Random ran = new Random(System.currentTimeMillis());
	    	long threadEndTime = System.currentTimeMillis() + (duration * 1000);
	    	long startTime, endTime, elapsedTime;
	    	double ranNum = 0.0;
	    	boolean found = false;
	    	while(!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < threadEndTime)
	    	{
	    		if (thinkTime != 0) sleep(thinkTime);
	    		ranNum = ran.nextDouble();
	    		
	    		for (int i=0;i<ratio.length && !found;i++)
	    		{
	    			if (ranNum < ratio[i])
	    			{
	    				Method m = functionList.get(i);
	    				startTime = System.currentTimeMillis();
	    				completed = (Boolean)m.invoke(benchObj);
	    				endTime = System.currentTimeMillis();
	    				elapsedTime = endTime - startTime; // getting response time.
	    				
	    				// save benchmark results for current thread.
	    				if (completed.booleanValue())
	    				{
	    					this.monitor.benchComplete(i);
	    					this.monitor.addResponseTime(i, elapsedTime);
	    				}
	    				else
	    				{
	    					this.monitor.benchFailed(i);
	    				}
	    				/*
	    				if (Thread.currentThread().isAlive() && monitor.measureStarted() && completed.booleanValue())
	    					writer.writeResponseTimeResult(Thread.currentThread().getName(), m.getName(), elapsedTime);
	    				*/
	    				found = true; 
	    			}
	    		}
	    		found = false;
	    	}
	    	
	    	if (resultPerThread)
	    	{
	    		this.writer.startWriteResponse(System.currentTimeMillis());
	    		for (int i=0;i<funcName.length;i++)
	    		{
	    			long rt = this.monitor.getResponseTime(i);
	    			long rt_sq = this.monitor.getResponseTimeSq(i);
	    			long n = this.monitor.getBenchCompleted(i);
	    			float mean = (float)rt/(float)n;
	    			double stddev = ((float)rt_sq/(float)n - (float)Math.pow((double)mean, 2.0));
	    			stddev = Math.sqrt(stddev);
	    			this.writer.writeResponseTimeResult(funcName[i], mean, stddev);
	    		}
	    		long rt = this.monitor.getTotalResponseTime();
    			long rt_sq = this.monitor.getTotalResponseTimeSq();
    			long n = this.monitor.getTotalBenchCompleted();
    			float mean = (float)rt/(float)n;
    			double stddev = ((float)rt_sq/(float)n - (float)Math.pow((double)mean, 2.0));
    			stddev = Math.sqrt(stddev);
    			this.writer.writeResponseTimeResult("total", mean, stddev);
	    	}
	    	// reports the final aggregate results to DBPEF class.
	    	for (int i=0;i<functionList.size();i++)
	    	{
	    		DBPEF.addBenchCompleted(i, this.monitor.getBenchCompleted(i));
	    		DBPEF.addBenchFailed(i, this.monitor.getBenchFailed(i));
	    		DBPEF.addResponseTime(i, this.monitor.getResponseTime(i));
	    		DBPEF.addResponseTimeSq(i, this.monitor.getResponseTimeSq(i));
	    	}
	    		logger.log().info("Thread: " + 
					Thread.currentThread().getName() + " is terminated.");
		}
		catch(Exception e)
		{
			logger.log().severe("Exception raised during PEFRunner thread run.");
			e.printStackTrace();
		}
	}
}
