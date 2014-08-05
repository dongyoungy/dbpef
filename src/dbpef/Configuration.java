/*
 *  Configuration.java
 *  
 *  Load a configuration as properties from a XML file.
 *  
 *  @author Danny Dong Young Yoon
 */

package dbpef;

import java.io.InputStream;
import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class Configuration {
	
	/* Local variables */
	private Properties prop; // Properties object storing benchmark configuration
	private PEFLogger logger; // Singleton logger
	
	/**
	 * Default constructor
	 */
    public Configuration() {
		init();    	
    }
    
    /**
     * Constructor with a Properties object as a parameter. This constructor is used 
     * by a DBPEFClient where configuration details are received as a Properties
     * object over the network.
     * @param p Properties object storing benchmark configuration
     */
    public Configuration(Properties p) {
    	prop = p;
    }
    
    /**
     * Initialise Properties object by loading a XML file.
     */
    private void init() {
    	try
    	{
    		logger = PEFLogger.INSTANCE;
    		prop = new Properties();
    		ClassLoader loader = ClassLoader.getSystemClassLoader();
    		InputStream is = loader.getResourceAsStream("config.xml");
    		if (is == null)
    		{
    			logger.warning("Configuration file not found.");
    			logger.info("Loading default configuration file...");
    			is = loader.getResourceAsStream("default.xml");
    		}
    		   		
    		if (is == null)
    		{
    			logger.severe("Could not load the default configuration.");
    			logger.severe("Terminating the framework...");
    			System.exit(1);
    		}
    		ConfigParser handler = new ConfigParser();
    		SAXParserFactory factory = SAXParserFactory.newInstance();
    		try
    		{
    			SAXParser saxParser = factory.newSAXParser();
    			saxParser.parse(is, handler);
    		}
    		catch (Exception e)
    		{
    			logger.severe("Failed to parse the XML configuration file.");
    			System.exit(1);
    		}
    		
    		prop = handler.getConfig();
       	}
    	catch (Exception e)
    	{
    		logger.log().severe("IOException: Failed to read configuration file");
    		e.printStackTrace();
    	}
    }
    
    /**
     * Returns the value of properties for a given key.
     * @param key Properties key for value.
     * @return Value value paired with the key given in the Properties object.
     */
    public String getProp(String key) {
    	return prop.getProperty(key);
    }
    
    /**
     * Returns the Properties object storing benchmark configuration.
     * @return Properties object.
     */
    public Properties getProperties() {
    	return this.prop;
    }
}
