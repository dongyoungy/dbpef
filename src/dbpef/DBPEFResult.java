package dbpef;

import java.io.Serializable;
import java.util.ArrayList;

public class DBPEFResult implements Serializable {
	
	private int interval;
	private long startTime;
	private String[] funcName;
	private long[] benchCompleted;
	private long[] benchFailed;
	private long[] responseTime;
	private long[] responseTimeSq;
	private ArrayList<float[]> throughputList;
	private ArrayList<Float> totalThroughputList;
	
	public DBPEFResult(int interval, int numBenchFunction) {
		this.interval = interval;
		throughputList = new ArrayList<float[]>();
		totalThroughputList = new ArrayList<Float>();
		benchCompleted = new long[numBenchFunction];
		benchFailed = new long[numBenchFunction];
		responseTime = new long[numBenchFunction];
		responseTimeSq = new long[numBenchFunction];
	}
	
	public void add(float[] throughput) {
		float[] val = new float[throughput.length];
		for (int i=0;i<val.length;i++)
		{
			val[i] = throughput[i];
		}
		throughputList.add(val);
	}
	
	public void addTotal(float totalThroughput) {
		totalThroughputList.add(new Float(totalThroughput));
	}
	
	public void setStartTime(long startTime)
	{
		this.startTime = startTime;
	}
	public void setFunctionName(String[] name) {
		funcName = name;
	}
	public void setBenchCompleted(long[] complete) {
		for (int i=0;i<benchCompleted.length;i++)
		{
			benchCompleted[i] = complete[i];
		}
	}
	public void setBenchFailed(long[] failed) {
		for (int i=0;i<benchFailed.length;i++)
		{
			benchFailed[i] = failed[i];
		}
	}
	public void setResponseTime(long[] response) {
		for (int i=0;i<responseTime.length;i++)
		{
			responseTime[i] = response[i];
		}
	}
	public void setResponseTimeSq(long[] responseSq) {
		for (int i=0;i<responseTimeSq.length;i++) {
			responseTimeSq[i] = responseSq[i];
		}
	}
	public ArrayList<float []> getThroughput() {
		return throughputList;
	}
	
	public ArrayList<Float> getTotalThroughput() {
		return totalThroughputList;
	}
	public long[] getBenchComplated() {
		return benchCompleted;
	}
	public long[] getBenchFailed() {
		return benchFailed;
	}
	public long[] getResponseTime() {
		return responseTime;
	}
	public long[] getResponseTimeSq() {
		return responseTimeSq;
	}
	public int getInterval() {
		return interval;
	}
	public String[] getFunctionName() {
		return funcName;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
}
