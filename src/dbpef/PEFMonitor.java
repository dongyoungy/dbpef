/*
 * 	PEFMonitor.java
 * 
 * 	PEFMonitor records each benchmark transaction results.
 * 
 * 	@author Danny Dong Young Yoon
 */

package dbpef;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PEFMonitor {
	
	/* Local variables */
	private long[] benchCompleted;
	private long[] benchFailed;
	private long[] responseTime;
	private long[] responseTimeSq;
	private long totalBenchCompleted;
	private long totalBenchFailed;
	private long totalResponseTime;
	private long totalResponseTimeSq;
	private static long measureStartTime;
	private int numBench;
	private static boolean measureStart = false;
	
	private FileWriter tempFw;
	private PrintWriter tempPw;
	
	/**
	 * Default constructor with the number of benchmark function as a parameter
	 * @param numBench number of benchmark functions
	 */
	public PEFMonitor(int numBench) {
		
		this.numBench = numBench;
		benchCompleted = new long[numBench];
		benchFailed = new long[numBench];
		responseTime = new long[numBench];
		responseTimeSq = new long[numBench];
		init();
		
	}
	
	public PEFMonitor(int numBench, String threadName) {
		
		this.numBench = numBench;
		benchCompleted = new long[numBench];
		benchFailed = new long[numBench];
		responseTime = new long[numBench];
		responseTimeSq = new long[numBench];
		
		try
		{
			tempFw = new FileWriter(new File("resResult_" + threadName + ".txt" ));
			tempPw = new PrintWriter(tempFw);
		}
		catch (IOException e)
		{
			
		}
		
		
		init();
		
	}
	
	/**
	 * Initialises its local variables.
	 */
	public void init() {
				
		for (int i=0;i<numBench;i++) 
		{
			benchCompleted[i] = 0;
			benchFailed[i] = 0;
			responseTime[i] = 0;
			responseTimeSq[i] = 0;
		}
		totalResponseTime = 0;
		totalResponseTimeSq = 0;
		totalBenchFailed = 0;
		totalBenchCompleted = 0;
		
	}
	
	/**
	 * Invoked when a target DB reaches a steady-state. Sets measureStart to true
	 * and gets starting time of the performance evaluation.
	 */
	public static void startMeasure(long startTime){
		
		measureStart = true;
		measureStartTime = startTime;
		
	}
	
	/**
	 * Returns a starting time of the performance evaluation.
	 * @return a starting time of the performance evaluation.
	 */
	public static long getMeasureStartTime() {
		return measureStartTime;
	}
	
	/**
	 * Returns true if measurement has been started.
	 * @return true - if measurement has been started.
	 */
	public boolean measureStarted()	{
		
		return measureStart;
		
	}
	/**
	 * Increment the number of a successful benchmark function by 1.
	 * @param index index corresponding to the successful benchmark function.
	 */
	public void benchComplete(int index) {
		
		benchCompleted[index]++;
		totalBenchCompleted++;
		
	}
	/**
	 * Increment the number of a failed benchmark function by 1.
	 * @param index index corresponding to the failed benchmark function.
	 */
	public void benchFailed(int index) {
		
		benchFailed[index]++;
		totalBenchFailed++;
		
	}
	/**
	 * Adds a response time of a benchmark function.
	 * @param index index corresponding to a benchmark function.
	 * @param time a response time of the benchmark function.
	 */
	public void addResponseTime(int index, long time) {
		
		long temp = time * time;
		
		responseTime[index] += time;
		responseTimeSq[index] += (time * time);
		totalResponseTime += time;
		totalResponseTimeSq += (time * time);
		tempPw.println(time);
		//tempPw.println(index + " " + time + " " + temp + " " + responseTimeSq[index]);
		tempPw.flush();
		
	}
	
	/**
	 * Returns the total response time of a benchmark function.
	 * @param index index corresponding to a benchmark function.
	 * @return the total response time of the benchmark function.
	 */
	public long getResponseTime(int index) {
		
		return responseTime[index];
		
	}
	
	public long getResponseTimeSq(int index) {
		
		return responseTimeSq[index];
		
	}
	
	/**
	 * Returns the total response time of all benchmark functions.
	 * @return the total response time
	 */
	public long getTotalResponseTime() {
		
		return totalResponseTime;
		
	}
	
	public long getTotalResponseTimeSq() {
		return totalResponseTimeSq;
	}
	
	/**
	 * Returns the number of a benchmark function completed successfully.
	 * @param index index corresponding to a benchmark function.
	 * @return the number of a benchmark function completed successfully.
	 */
	public long getBenchCompleted(int index) {
		
		return benchCompleted[index];
		
	}
	/**
	 * Returns the total number of benchmark functions completed.
	 * @return the total number of benchmark functions completed.
	 */
	public long getTotalBenchCompleted() {
		
		return totalBenchCompleted;
		
	}
	/**
	 * Returns the number of a benchmark function failed to complete. 
	 * @param index index corresponding to a benchmark function.
	 * @return the number of a benchmark function failed to complete.
	 */
	public long getBenchFailed(int index) {
		
		return benchFailed[index];
		
	}
	/**
	 * Returns the total number of benchmark functions failed to complete.
	 * @return the total number of benchmark functions failed to complete.
	 */
	public long getTotalBenchFailed() {
		
		return totalBenchFailed;
		
	}
}
