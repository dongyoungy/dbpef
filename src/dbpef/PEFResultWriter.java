/*
 * 	PEFResultWriter.java
 * 
 *  Writes the benchmark result of each transaction to a text file. It basically
 *  writes the name of a thread which run a benchmark function, the name of the
 *  benchmark function and its response time.
 *  
 *  As all DBPEFRunner threads write its result to a single text file, this class
 *  implements Singleton design pattern and corresponding methods are synchronised.
 *   
 * 	@author Danny Dong Young Yoon
 */

package dbpef;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PEFResultWriter {
	
	/* Singleton object */
	//public final static PEFResultWriter INSTANCE = new PEFResultWriter();
	/* Local variables */
	private PEFLogger logger;
	private String threadName;
	private String responseFileName;
	private File responseFile;
	private FileWriter responseFw;
	private PrintWriter responsePw;
	
	private String throughputFileName;
	private File throughputFile;
	private FileWriter throughputFw;
	private PrintWriter throughputPw;

	
	/**
	 * Constructor
	 */
	public PEFResultWriter() {
		this.threadName = "Total";
	}
	public PEFResultWriter (String threadName) {
		this.threadName = threadName;
	}
	
	public void startWriteThroughput(long startTime) {

		throughputFileName = "DBPEF_result_throughput_" + this.getStartTime(startTime) + "_" + threadName + ".txt";
		throughputFile = new File(throughputFileName);
		
		try
		{
			throughputFw = new FileWriter(throughputFile);
		}
		catch (IOException e)
		{
			logger.log().severe("IOException raised at PEFResultWriter: Failed to initialise FileWriter");
			e.printStackTrace();
		}
		throughputPw = new PrintWriter(throughputFw);
	}
	
	public void startWriteAvgThroughput() {

		throughputFileName = "DBPEFServer_result_avg_throughput_" + this.getStartTime(System.currentTimeMillis()) + "_" + threadName + ".txt";
		throughputFile = new File(throughputFileName);
		
		try
		{
			throughputFw = new FileWriter(throughputFile);
		}
		catch (IOException e)
		{
			logger.log().severe("IOException raised at PEFResultWriter: Failed to initialise FileWriter");
			e.printStackTrace();
		}
		throughputPw = new PrintWriter(throughputFw);
	}
	
	public void startWriteResponse(long startTime) {
		
		responseFileName = "DBPEF_result_response_time_" + this.getStartTime(startTime) + "_" + threadName + ".txt";
		responseFile = new File(responseFileName);
		
		try
		{
			responseFw = new FileWriter(responseFile);
		}
		catch (IOException e)
		{
			logger.log().severe("IOException raised at PEFResultWriter: Failed to initialise FileWriter");
			e.printStackTrace();
		}
		responsePw = new PrintWriter(responseFw);
		responsePw.println("BenchmarkFunction,Mean,StdDev");
	}
	
	/**
	 * Writes a benchmark result to a text file.
	 * @param methodName a name of a benchmark function
	 * @param elapsedTime a response time of a benchmark function
	 */
	public void writeResponseTimeResult(String methodName, float mean, double stddev) {
		
		responsePw.printf("%s,%.5f,%.5f %n", methodName, mean, stddev);
		responsePw.flush();
		
	}
	
	public void writeThroughputHeader(String[] funcName) {
		throughputPw.print("Time");
		for (int i=0;i<funcName.length;i++)
		{
			throughputPw.printf(",%s",funcName[i]);
		}
		throughputPw.println(",Total");
		throughputPw.flush();
	}
	
	public void writeThroughputResult(long time, float[] throughput, float totalThroughput)
	{
		throughputPw.flush();
		throughputPw.printf("%d", time);
		for (int i=0;i<throughput.length;i++)
		{
			throughputPw.printf(",%.5f", throughput[i]);
		}
		throughputPw.printf(",%.5f%n", totalThroughput);
		throughputPw.flush();
	}
	
	public void writeMeasureStartingTime(long measureStartTime) {
		
		throughputPw.printf("%d%n",measureStartTime);
		throughputPw.flush();
	}
	
	/**
	 * returns a starting of the benchmark which is used in the name of a text file.
	 * @return a starting time of the benchmark
	 */
	private String getStartTime()
	{
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmm");
		Date date = new Date();
		return df.format(date);
	}
	
	private String getStartTime(long currentTime)
	{
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmm");
		Date date = new Date(currentTime);
		return df.format(date);
	}
}
