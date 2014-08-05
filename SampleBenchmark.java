/**
 * @(#)SampleBenchmark.java
 *
 *
 * @author 
 * @version 1.00 2008/8/15
 */

import java.io.Serializable;

public class SampleBenchmark implements Serializable{

    public SampleBenchmark() {
    }
    
	public void init() {
		System.out.println("init()");
	}
	
	public void tx_1() {
		tx1helper();
		
	}
	
	private void tx1helper() {
		System.out.println("tx_1()");
	}
	
	public void tx_2() {
		System.out.println("tx_2()");
	}
	
	public void teardown() {
		System.out.println("teardown()");
	}
		    
}