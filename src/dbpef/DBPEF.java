/*
 *  DBPEF.java
 *  
 *  Database Performance Evaluation Framework (DBPEF)
 *  
 *  DBPEF class loads configuration and benchmark classes and runs 
 *  PEFRunner threads.
 *  
 *  @author Danny Dong Young Yoon
 */


package dbpef;

import java.lang.reflect.Method;
import java.util.ArrayList;

public class DBPEF {
	
	/* Local variables */
	protected Configuration config = null;
	protected String userModel;
	protected String singleFunc;
	protected String prefix;
	protected String initName;
	protected String tearDownName;
	protected ArrayList<Method> funcList = new ArrayList<Method>();
	protected PEFResultWriter writer;
	private Method initMethod = null;
	private Method tearDownMethod = null;
	private Method[] methList;
	protected boolean resultPerThread;
	protected String[] funcName;
	protected double[] ratio;
	protected Class benchClass;
	protected int numBenchFunction;
	protected int duration;
	protected static int MPL;
	protected int interval;
	protected long thinkTime;
	protected PEFLogger logger;
	
	/* Local variables storing aggregate benchmark results */
	protected static long[] benchCompleted;
	protected static long[] benchFailed;
	protected static long[] responseTime;
	protected static long[] responseTimeSq;
	protected long benchCompletionTime;
	protected long benchStartTime;
	
	/* Constants */
	protected static final char spins[] = { '-', '\\', '|', '/' };
	protected static final int CLIENT_NOT_READY = 0;
	protected static final int CLIENT_READY = 1;
	protected static final int CLIENT_STEADY = 2;
	protected static final int CLIENT_GO = 3;
	protected static final String GO_SIGNAL = "go";
	protected static final String serviceType = "_dbpef._tcp";
	protected static final String serviceDomain = "local";
	
	/**
	 * Constructor
	 */
	public DBPEF() {
		
		System.out.println();
		logger = PEFLogger.INSTANCE;
		writer = new PEFResultWriter();
	}
	
	/**
	 * Initialise a DBPEF object. It loads benchmark functions from a benchmark
	 * class specified in a configuration file along with its transaction mix and
	 * benchmark options.
	 */
	protected void initBenchmark() {
		
		methList = loadBenchmark();
		logger.fine("Benchmark class loading successful.");
		loadBenchmarkFunctions(methList);
		logger.fine("Benchmark function loading successful.");
		ratio = loadTxMix();
		logger.fine("Benchmark transaction mix loading successful.");
		logger.info("Benchmark loading successful.");
		benchCompleted = new long[numBenchFunction];
		benchFailed = new long[numBenchFunction];
		responseTime = new long[numBenchFunction];
		responseTimeSq = new long[numBenchFunction];
	}
	
	/**
	 * Runs a benchmark in a single mode, which means that there is only one 
	 * machine running the benchmark.
	 */
	public void runSingleMode() {
		
		logger.info("DBPEF has been started in a single mode...");
		config = new Configuration();
		this.initBenchmark();
		this.init(); // calls initialise function
		this.run(); // runs benchmark
		this.tearDown(); // calls tear down function.
	}
	
	/**
	 * Runs the benchmark by creating PEFRunner Runnable objects and running them
	 * in order to simulate MPL specified by a user.
	 */
	protected void run() {
		
		boolean steadyState = false;
		boolean measureStart = false;
		float[] currentThroughput = new float[numBenchFunction];
		float totalThroughput = 0;
		float lastTotalThroughput = 0;
		long[] current = new long[numBenchFunction];
		long[] last = new long[numBenchFunction];
		int spinCycle = 0;
		int intervalCycle = 0;
		int numInterval = 0;
		long measureStartTime = 0;
		
		writer.startWriteThroughput(System.currentTimeMillis());
		ThreadGroup runners = new ThreadGroup("PEFRunners");
		PEFRunner runner[] = new PEFRunner[MPL];
		writer.writeThroughputHeader(funcName);
		
		System.out.println();
		if (userModel.toLowerCase().equals("mix")) logger.info("User model : Transaction mix");
		else logger.info("User model : Single transaction (" + singleFunc + ")");
		if (thinkTime > 0) logger.info("Think time is " + thinkTime + " ms." );
		else logger.info("Think time is 0 ms.");
		
		try
		{
			for (int i=0;i<MPL;i++)
			{
				runner[i] = new PEFRunner(runners, "Thread" + (i + 1), benchClass.newInstance(),
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
						measureStartTime = (numInterval + 1) * interval;
						long responseMeasureStartTime = System.currentTimeMillis();
						writer.startWriteResponse(responseMeasureStartTime);
						PEFMonitor.startMeasure(responseMeasureStartTime);
						for (int k=0;k<numBenchFunction;k++)
						{
							last[k] = 0;
						}
						System.out.println();
						logger.info("Steady-state reached: Benchmark measurement started.");
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
					if (steadyState && !measureStart)
					{
						for (int i=0;i<MPL;i++)
						{
							runner[i].startMeasure();
						}
						measureStart = true;
					}
					writer.writeThroughputResult((numInterval + 1) * interval, currentThroughput, totalThroughput);
					numInterval++;
					intervalCycle = 0;
				}
				if (!steadyState)
				{
					System.out.print(".");
				}
				else
				{
					System.out.print("DBPEF is now running... " + spins[spinCycle] + "\b\r");
					spinCycle = (spinCycle + 1) & 3;
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		try
		{
			Thread.sleep(1000);
		}
		catch (Exception e)
		{
			
		}
		
		for (int i=0;i<funcName.length;i++)
		{
			long rt = responseTime[i];
			long rt_sq = responseTimeSq[i];
			System.out.println(rt_sq);
			long n = benchCompleted[i];
			float mean = (float)rt/(float)n;
			double stddev = ((float)rt_sq/(float)(n) - (float)Math.pow((double)mean, 2.0));
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
		double stddev = ((double)rt_sq/(double)(n) - (double)(mean * mean));
		stddev = Math.sqrt(stddev);
		System.out.println(rt + " " + rt_sq + " " + n + " " + mean + " " + stddev);
		this.writer.writeResponseTimeResult("Total", mean, stddev);
		
		writer.writeMeasureStartingTime(measureStartTime);
		benchCompletionTime = System.currentTimeMillis();
		benchStartTime = PEFMonitor.getMeasureStartTime();
		System.out.println();
		logger.info("DBPEF completed.");
		try 
		{
			Thread.sleep(1);
			this.printStat();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Prints the statistic of benchmark results.
	 */
	protected void printStat(){
		
		long benchDuration = benchCompletionTime - benchStartTime;
		long totalBenchCompleted = 0;
		long totalBenchFailed = 0;
		long totalResponseTime = 0;
		
		for (int i=0;i<numBenchFunction;i++)
		{
			totalBenchCompleted += benchCompleted[i];
			totalBenchFailed += benchFailed[i];
			totalResponseTime += responseTime[i];
		}
		
		System.out.println("");
		System.out.println("=============================================================");
		System.out.println("                    Benchmark   Summary");
		System.out.println("=============================================================");
		System.out.printf(" Duration of measurement interval      = %2.2f %s\n",(float)benchDuration/1000.0, "seconds");
		System.out.println(" Total number of transaction completed = " + totalBenchCompleted);
		System.out.println(" Total number of transaction failed    = " + totalBenchFailed);
		System.out.printf(" Error Rate                            = %2.2f %s\n", 
				100.0*(float)totalBenchFailed/(float)(totalBenchFailed+totalBenchCompleted), "%");
		System.out.println(" Total Response Time                   = " + totalResponseTime + " ms");
		System.out.printf(" Average Response Time                 = %2.2f %s\n", 
				(float)totalResponseTime/(float)totalBenchCompleted, "ms");
		System.out.printf(" Overall Throughput                    = %2.2f %s\n",
				(float)totalBenchCompleted/(float)benchDuration*1000.0,"tps");
		System.out.println("=============================================================");
		System.out.println("-------------------------------------------------------------");
		
		for (int i=0;i<numBenchFunction;i++)
		{
			Method m = funcList.get(i);
			System.out.println("      Benchmark function - " + m.getName());
			System.out.println("-------------------------------------------------------------");
			System.out.printf(" Transaction mix       = %2.2f %s\n", 100.0*(float)benchCompleted[i]/(float)totalBenchCompleted, "%");
			System.out.println(" Transaction completed = " + benchCompleted[i]);
			System.out.println(" Transaction failed    = " + benchFailed[i]);
			System.out.printf(" Error rate            = %2.2f %s\n", 100.0*(float)benchFailed[i]/(float)(benchCompleted[i]+benchFailed[i]), "%");
			System.out.println(" Total response Time   = " + responseTime[i] + " ms");
			System.out.printf(" Average response Time = %2.2f %s\n", (float)responseTime[i]/(float)benchCompleted[i] , "ms");
			System.out.printf(" Overall Throughput    = %2.2f %s\n",
					(float)benchCompleted[i]/(float)benchDuration*1000.0,"tps");
			System.out.println("-------------------------------------------------------------");
		}
		
	}
	
	/**
	 * Each PEFRunner object invokes this method to update the number of benchmark
	 * functions succeeded.
	 * @param index index of an array corresponding to a succeeded benchmark function.
	 * @param num the number of a succeeded benchmark function run.
	 */
	public static synchronized void addBenchCompleted(int index, long num) {
		
		benchCompleted[index] += num;
		
	}
	/**
	 * Each PEFRunner object invokes this method to update the number of benchmark
	 * functions failed.
	 * @param index index of an array corresponding to a single benchmark function.
	 * @param num the number of a failed benchmark function run.
	 */
	public static synchronized void addBenchFailed(int index, long num) {
		
		benchFailed[index] += num;
		
	}
	/**
	 * Each PEFRunner object invokes this method to update the total response time 
	 * of benchmark functions that are successful.
	 * @param index index of an array corresponding to a single benchmark function.
	 * @param num response time of a succeeded benchmark function run.
	 */
	public static synchronized void addResponseTime(int index, long num) {
		
		responseTime[index] += num;
		
	}
	
	/**
	 * Each PEFRunner object invokes this method to update the total response time
	 * squared of benchmark functions that are successful.
	 * @param index index of an array corresponding to a single benchmark function.
	 * @param num response time of a succeeded benchmark function run.
	 */
	public static synchronized void addResponseTimeSq(int index, long num) {
		
		responseTimeSq[index] += num;
		
	}
	
	/**
	 * Invokes inititalise method in a benchmark class, which is specified in 
	 * a configuration file. 
	 */
	protected void init() {
		try
		{
			System.out.println();
			logger.info("Initialising the target database...");
			initMethod.invoke(benchClass.newInstance());
			logger.info("Initialisation of the target database completed.");
		}
		catch(Exception e)
		{
			logger.severe("init() method failed.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Invokes tear down method in a benchmark class, which is specified in 
	 * a configuration file.
	 */
	protected void tearDown(){
		try
		{ 
			tearDownMethod.invoke(benchClass.newInstance());
			System.out.println();
			logger.info("The target database has been tear down.");
		}
		catch(Exception e)
		{
			logger.severe("tearDown() method failed.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Loads details of benchmark from a configuration file and benchmark class.
	 * @return an array of Method objects from a benchmark class. 
	 */
	private Method[] loadBenchmark() {
		try
		{
			benchClass = Class.forName(config.getProp("filename"));
			userModel = config.getProp("user_model") != null ? config.getProp("user_model"):"mix";
			if (userModel.toLowerCase().equals("single"))
			{
				singleFunc = config.getProp("single_function");
				if (singleFunc == null)
				{
					logger.log().severe("User selected single function user mode but DBPEF could not find the corresponding function.");
					System.out.println("ERROR: User selected single function user mode but DBPEF could not find the corresponding function.");
					System.exit(-1);
				}
			}
			// loads configuration details from a configuration file if the corresponding
			// element exists. If not, default values are assigned.
			prefix = config.getProp("benchmark_prefix");
			initName = config.getProp("init_name") != null ? config.getProp("init_name"):"init";
			tearDownName = config.getProp("tear_down_name") != null ? config.getProp("tear_down_name"):"init";
			duration = Integer.parseInt(config.getProp("duration") != null ? config.getProp("duration"):"1800");
			MPL = Integer.parseInt(config.getProp("MPL") != null ? config.getProp("MPL"):"10");
			interval = Integer.parseInt(config.getProp("interval") != null ? config.getProp("interval"):"30");
			thinkTime = Integer.parseInt(config.getProp("think_time") != null ? config.getProp("think_time"):"0");
			if (config.getProp("result_per_thread") == null) resultPerThread = false;
			else if (config.getProp("result_per_thread").equalsIgnoreCase("true")) resultPerThread = true;
			else resultPerThread = false;
		}
		catch (ClassNotFoundException e)
		{
			logger.log().severe("Benchmark class not found.");
			logger.log().severe("Terminating the framework...");
			System.exit(1);
		}
		
		return benchClass.getDeclaredMethods();
	}
	
	/**
	 * Loads benchmark functions from Method objects from a benchmark class 
	 * according to user model specified in a configuration file. (transaction mix / single function)
	 * @param mList an array of Method objects of declared methods in a benchmark class.
	 */
	private void loadBenchmarkFunctions(Method[] mList) {
		
		if (userModel.toLowerCase().equals("single"))
		{
			for (int i=0;i<mList.length;i++)
			{
				String name = mList[i].getName();
				Method m = mList[i];
				if (name.toLowerCase().equals(singleFunc.toLowerCase())) funcList.add(m);
				if (name.toLowerCase().compareTo(initName.toLowerCase()) == 0) initMethod = m;
				if (name.toLowerCase().compareTo(tearDownName.toLowerCase()) == 0) tearDownMethod = m;
			}
		}
		else
		{
			for (int i=0;i<mList.length;i++)
			{
				String name = mList[i].getName();
				Method m = mList[i];
				if (name.startsWith(prefix)) funcList.add(m);
				if (name.toLowerCase().compareTo(initName.toLowerCase()) == 0) initMethod = m;
				if (name.toLowerCase().compareTo(tearDownName.toLowerCase()) == 0) tearDownMethod = m;
			}
		}
		if (funcList.size() < 1)
		{
			logger.log().severe("DBPEF could not find benchmark functions with given prefix.");
			System.out.println("ERROR: DBPEF could not find benchmark functions with given prefix.");
			System.exit(-1);
		}
		else if (initMethod == null)
		{
			logger.log().severe("DBPEF could not find an initialize function.");
			System.out.println("ERROR: DBPEF could not find an initialize function.");
			System.exit(-1);
		}
		else if (tearDownMethod == null)
		{
			logger.log().severe("DBPEF could not find a teardown function.");
			System.out.println("ERROR: DBPEF could not find a teardown function.");
			System.exit(-1);
		}
		
		numBenchFunction = funcList.size();
		
		funcName = new String[numBenchFunction];
		
		for (int i=0;i<numBenchFunction;i++)
		{
			funcName[i] = funcList.get(i).getName();
		}
	}
	
	/**
	 * Loads ratios for each transaction in a transaction mix from a configuration
	 * file. 
	 * @return an array of transaction mix ratios which index corresponds to its benchmark function.
	 */
	private double[] loadTxMix() {
		double accumRatio = 0.0;
		double[] ratio = new double[numBenchFunction];
		for (int i=0;i<numBenchFunction;i++)
		{
			Method m = funcList.get(i);
			String name = m.getName();
			
			accumRatio += Double.parseDouble(config.getProp(name + "_mix_ratio"));
			ratio[i] = accumRatio;
		}
		
		if (accumRatio > 1.0)
		{
			for (int i=0;i<numBenchFunction;++i)
			{
				ratio[i] = ratio[i] / accumRatio;
			}
		}
		return ratio;
	}
}
