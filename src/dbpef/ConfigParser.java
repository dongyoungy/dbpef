/*
 * 	ConfigParser.java
 *	
 *	Reads a XML configuration file and parses it using SAX 
 *  into properties object.
 *
 * 	@author Danny Dong Young Yoon
 */

package dbpef;

import java.util.Properties;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class ConfigParser extends DefaultHandler {
	
	/* Local variables */
	private Properties prop;
	private String currentKey = null;
	
	/**
	 * Default constructor
	 */
	public ConfigParser () {
		prop = new Properties();
	}
	
	/**
	 * Returns configuration properties object.
	 * @return properties object having configuration values.
	 */
	public Properties getConfig() {
		return prop;
	}
	
	/**
	 * Automatically invoked by SAX parser when there is a start element
	 * tag in a XML configuration file.
	 */
	public void startElement(String namespaceURI, String localName,
			String key, Attributes atts) throws SAXException {
		if (key.equalsIgnoreCase("filename"))
			currentKey = "filename";
		else if (key.equalsIgnoreCase("init_name"))
			currentKey = "init_name";
		else if (key.equalsIgnoreCase("tear_down_name"))
			currentKey = "tear_down_name";
		else if (key.equalsIgnoreCase("benchmark_prefix"))
			currentKey = "benchmark_prefix";
		else if (key.equalsIgnoreCase("duration"))
			currentKey = "duration";
		else if (key.equalsIgnoreCase("interval"))
			currentKey = "interval";
		else if (key.equalsIgnoreCase("MPL"))
			currentKey = "MPL";
		else if (key.equalsIgnoreCase("think_time"))
			currentKey = "think_time";
		else if (key.equalsIgnoreCase("user_model"))
			currentKey = "user_model";
		else if (key.equalsIgnoreCase("single_function"))
			currentKey = "single_function";
		else if (key.equalsIgnoreCase("result_per_thread"))
			currentKey = "result_per_thread";
		else currentKey = key;
	}
	
	/**
	 * Automatically invoked by SAX parser when there is a value.
	 */
	public void characters(char ch[], int start, int length) throws SAXException{
		
		String value = new String(ch, start, length);
		
		if (!value.trim().equals("")) 
		{
			prop.setProperty(currentKey, value);
		}
	}
}
