/*
 *  PEFLogger.java
 *  
 *  A singleton class that records logs during the execution of DBPEF.
 *  This class records logs into unique text files as well as printing out to screen.
 *  
 *  @author Danny Dong Young Yoon
 */

package dbpef;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class PEFLogger {
	
	/* Local variables */
	public final static PEFLogger INSTANCE = new PEFLogger(); // Singleton.
	private Logger log; // Logger object used in writing logs.
	
	/**
	 * Default constructor
	 */
	private PEFLogger()	{
		log = Logger.getLogger("DBPEF");
		log.setUseParentHandlers(false);
		log.setLevel(Level.ALL);
		try
		{
			FileHandler fh = new FileHandler("DBPEF_" + getLogStartTime() + "_%g.log",1000000,10,true);
			fh.setFormatter(new SimpleFormatter());
			log.addHandler(fh);
		}
		catch (IOException e)
		{
			log.severe("Failed to initialize FileHandler");
			e.printStackTrace();
		}
		log.info("Logging has been started.");
	}
	
	/**
	 * Returns a starting time of logging which then uses to create a log file.
	 * @return a starting time of logging.
	 */
	private String getLogStartTime()
	{
		SimpleDateFormat df = new SimpleDateFormat("ddMMyyyy_HHmmss");
		Date date = new Date();
		return df.format(date);
	}
	
	/**
	 * Returns a logger object.
	 * @return a logger object.
	 */
	public Logger log()
	{
		return log;
	}
	
	/**
	 * Records a given String as a FINE log message and prints it out to console.
	 * @param s a log message
	 */
	public void fine(String s) {
		System.out.println(s);
		log.fine(s);
	}
	/**
	 * Records a given String as a INFO log message and prints it out to console.
	 * @param s a log message
	 */
	public void info(String s) {
		System.out.println(s);
		log.info(s);
	}
	/**
	 * Records a given String as a SEVERE log message and prints it out to console.
	 * @param s a log message
	 */
	public void severe(String s) {
		System.out.println(s);
		log.severe(s);
	}
	/**
	 * Records a given String as a WARNING log message and prints it out to console.
	 * @param s a log message
	 */
	public void warning(String s) {
		System.out.println(s);
		log.warning(s);
	}
}
