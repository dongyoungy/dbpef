/*
 * 	DBPEFMain.java
 * 
 * 	Driver for DBPEF. Runs DBPEF either in a single or distributed mode according 
 * 	to user's command line arguments.
 * 
 * 	@author Danny Dong Young Yoon
 */

package dbpef;

public class DBPEFMain {
	
	public static void main(String[] args){
		
		if (args.length == 1 && args[0].equalsIgnoreCase("single"))
		{
			try
			{
				DBPEF dbpef = new DBPEF();
				dbpef.runSingleMode(); // run as single mode
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		else if (args.length == 1 && args[0].equalsIgnoreCase("server"))
		{
			new DBPEFServer(); // run as server
		}
		else if (args.length == 1 && args[0].equalsIgnoreCase("client"))
		{
			new DBPEFClient(); // run as client
		}
		else 
		{
			System.out.println("Usage: java dbpef.DBPEFMain single          => for sinle mode");
			System.out.println("Usage: java dbpef.DBPEFMain server|client   => for distributed mode");
		}
		
	}
	
}
