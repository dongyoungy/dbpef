/**
 * @(#)SampleTPCC.java
 *
 *
 * @author 
 * @version 1.00 2008/8/25
 */


import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.text.*;

public class SampleTPCC_OverheadTest {
	
	private Connection conn = null;
    private Statement stmt = null;
    private Statement stmt1 = null;
    private ResultSet rs = null;
    private int terminalWarehouseID, terminalDistrictID;
	
	private Random  gen;
    private int transactionCount = 1, numTransactions, numWarehouses, newOrderCounter;
    private StringBuffer query = null;
    private int result = 0;
    private boolean stopRunningSignal = false;
	
	public final static String JTPCCVERSION = "2.3.2";

    public final static boolean OUTPUT_MESSAGES = true;
    public final static boolean TERMINAL_MESSAGES = true;
    public final static int NEW_ORDER = 1, PAYMENT = 2, ORDER_STATUS = 3, DELIVERY = 4, STOCK_LEVEL = 5;

    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public final static String terminalPrefix = "Term-";
    public final static String reportFilePrefix = "reports/BenchmarkSQL_session_";


    // these values can be overridden with command line parms
    public final static String defaultDatabase = "jdbc:edb://localhost/edb";
    public final static String defaultUsername = "enterprisedb";
    public final static String defaultPassword = "password";
    public final static String defaultDriver = "com.edb.Driver";

	public final static String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	public final static String url = "jdbc:sqlserver://localhost:3232;DatabaseName=DBPEF";

    public final static String defaultNumWarehouses = "1";
    public final static String defaultNumTerminals = "1";

    public final static String defaultPaymentWeight = "43";
    public final static String defaultOrderStatusWeight = "4";
    public final static String defaultDeliveryWeight = "4";
    public final static String defaultStockLevelWeight = "4";

    public final static String defaultTransactionsPerTerminal = "1";
    public final static String defaultMinutes = "1";
    public final static boolean defaultRadioTime = true;
    public final static boolean defaultDebugMessages = false;
    public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public final static int  configCommitCount  = 1000;  // commit every n records in LoadData
    public final static int  configWhseCount    = 1;
    public final static int  configItemCount    = 100000; // tpc-c std = 100,000
    public final static int  configDistPerWhse  = 10;     // tpc-c std = 10
    public final static int  configCustPerDist  = 3000;   // tpc-c std = 3,000
	
	private static PreparedStatement  custPrepStmt;
  	private static PreparedStatement  distPrepStmt;
  	private static PreparedStatement  histPrepStmt;
  	private static PreparedStatement  itemPrepStmt;
  	private static PreparedStatement  nworPrepStmt;
  	private static PreparedStatement  ordrPrepStmt;
  	private static PreparedStatement  orlnPrepStmt;
  	private static PreparedStatement  stckPrepStmt;
  	private static PreparedStatement  whsePrepStmt;
	
	private static java.sql.Timestamp sysdate    = null;
	private static java.util.Date     now        = null;
	private static String             dbType;
    private static String             fileLocation  = "";
    private static boolean            outputFiles   = false;
    private static PrintWriter        out           = null;
    private static long               lastTimeMS    = 0;
	
	//NewOrder Txn
    private PreparedStatement stmtGetCustWhse = null;
    private PreparedStatement stmtGetDist = null;
    private PreparedStatement stmtInsertNewOrder = null;
    private PreparedStatement stmtUpdateDist = null;
    private PreparedStatement stmtInsertOOrder = null;
    private PreparedStatement stmtGetItem = null;
    private PreparedStatement stmtGetStock = null;
    private PreparedStatement stmtUpdateStock = null;
    private PreparedStatement stmtInsertOrderLine = null;

    //Payment Txn
    private PreparedStatement payUpdateWhse = null;
    private PreparedStatement payGetWhse = null;
    private PreparedStatement payUpdateDist = null;
    private PreparedStatement payGetDist = null;
    private PreparedStatement payCountCust = null;
    private PreparedStatement payCursorCustByName = null;
    private PreparedStatement payGetCust = null;
    private PreparedStatement payGetCustCdata = null;
    private PreparedStatement payUpdateCustBalCdata = null;
    private PreparedStatement payUpdateCustBal = null;
    private PreparedStatement payInsertHist = null;

    //Order Status Txn
    private PreparedStatement ordStatCountCust = null;
    private PreparedStatement ordStatGetCust = null;
    private PreparedStatement ordStatGetNewestOrd = null;
    private PreparedStatement ordStatGetCustBal = null;
    private PreparedStatement ordStatGetOrder = null;
    private PreparedStatement ordStatGetOrderLines = null;
    
    //Delivery Txn
    private PreparedStatement delivGetOrderId = null;
    private PreparedStatement delivDeleteNewOrder = null;
    private PreparedStatement delivGetCustId = null;
    private PreparedStatement delivUpdateCarrierId = null;
    private PreparedStatement delivUpdateDeliveryDate = null;
    private PreparedStatement delivSumOrderAmount = null;
    private PreparedStatement delivUpdateCustBalDelivCnt = null;

    //Stock Level Txn
    private PreparedStatement stockGetDistOrderId = null;
    private PreparedStatement stockGetCountStock = null;

    public SampleTPCC_OverheadTest() {
  
  		try
  		{
  			Class.forName(driver);
			//DriverManager.registerDriver(new com.microsoft.jdbc.sqlserver.SQLServerDriver());
			this.conn=DriverManager.getConnection(url,"sa","1234");
			conn.setAutoCommit(false);
			/*
			SQLServerDataSource ds = new SQLServerDataSource();
			ds.setUser("sa");
			ds.setPassword("1234");
			ds.setServerName("localhost");
			ds.setDatabaseName("PDV");
			ds.setPortNumber(3232);
			ds.setServerName("PC-4E65-0\\SQLEXPRESS");
			ds.setIntegratedSecurity(true);
			*/
			//this.conn = ds.getConnection();
  			
  			gen = new Random(System.currentTimeMillis());  	
	        this.stmt = this.conn.createStatement();
	        this.stmt.setMaxRows(200);
	        this.stmt.setFetchSize(100);
	
	        this.stmt1 = conn.createStatement();
	        this.stmt1.setMaxRows(1);
	        
	        this.numWarehouses = configWhseCount;
	        this.terminalWarehouseID = 1;
	        this.newOrderCounter = 0;
  		}
  		catch (Exception e)
  		{
  			e.printStackTrace();
  		}
    }
    
    // ************************** Helper Methods *****************************
    
    public static String randomStr(long strLen){
	
	    char freshChar;
	    String freshString;
	    freshString="";
	
	    while(freshString.length() < (strLen - 1)){
	
	      freshChar= (char)(Math.random()*128);
	      if(Character.isLetter(freshChar)){
	        freshString += freshChar;
	      }
	    }
	
	    return (freshString);

	} // end randomStr


    private String getCurrentTime()
    {
	    return dateFormat.format(new java.util.Date());
    }

    private String formattedDouble(double d)
    {
        String dS = ""+d;
        return dS.length() > 6 ? dS.substring(0, 6) : dS;
    }    

    private int getItemID(Random r)
    {
        return nonUniformRandom(8191, 1, 100000, r);
    }

    private int getCustomerID(Random r)
    {
        return nonUniformRandom(1023, 1, 3000, r);
    }

    private String getLastName(Random r)
    {
        int num = (int)nonUniformRandom(255, 0, 999, r);
        return nameTokens[num/100] + nameTokens[(num/10)%10] + nameTokens[num%10];
    }

    private int randomNumber(int min, int max, Random r)
    {
        return (int)(r.nextDouble() * (max-min+1) + min);
    }


    private int nonUniformRandom(int x, int min, int max, Random r)
    {
        return (((randomNumber(0, x, r) | randomNumber(min, max, r)) + randomNumber(0, x, r)) % (max-min+1)) + min;
    }
    
    private void printMessage(String message){
        //if(debugMessages) terminalOutputArea.println("[ jTPCC ] " + message);
    }
	private void terminalMessage(String message)
    {
        //if(TERMINAL_MESSAGES) terminalOutputArea.println(message);
    }

  	void transRollback () {
      	try 
      	{
        	conn.rollback();
      	} 
      	catch(SQLException se) 
      	{
        	System.out.println(se.getMessage());
      	}
  	}

 	void transCommit() {
    	try {
         	conn.commit();
   		} catch(SQLException se) {
        	System.out.println(se.getMessage());
        	transRollback();
      	} 

	} // end transCommit()
	
	private void insertOrder(PreparedStatement ordrPrepStmt, int o_id, int o_w_id, int o_d_id, int o_c_id, int o_carrier_id,
		int o_ol_cnt, int o_all_local, long o_entry_d) {
    
		try
		{
		
			ordrPrepStmt.setInt(1, o_id);
			ordrPrepStmt.setInt(2, o_w_id);
			ordrPrepStmt.setInt(3, o_d_id);
			ordrPrepStmt.setInt(4, o_c_id);
			ordrPrepStmt.setInt(5, o_carrier_id);
			ordrPrepStmt.setInt(6, o_ol_cnt); 
			ordrPrepStmt.setInt(7, o_all_local);
			Timestamp entry_d = new java.sql.Timestamp(o_entry_d);
			ordrPrepStmt.setTimestamp(8, entry_d);
			
			ordrPrepStmt.addBatch();
		
		} catch(SQLException se) { 
			System.out.println(se.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}  // end insertOrder()
	
	private void insertNewOrder(PreparedStatement nworPrepStmt, int no_w_id, int no_d_id, int no_o_id) {
    
        try {
          nworPrepStmt.setInt(1, no_w_id); 
          nworPrepStmt.setInt(2, no_d_id); 
          nworPrepStmt.setInt(3, no_o_id); 

          nworPrepStmt.addBatch();              
          
      } catch(SQLException se) { 
        System.out.println(se.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
       }

    }
    
    private void insertOrderLine(PreparedStatement orlnPrepStmt, int ol_w_id, int ol_d_id, int ol_o_id, int ol_number,
    	int ol_i_id, long ol_delivery_d, float ol_amount, int ol_supply_w_id, int ol_quantity, String ol_dist_info) {
    
      try {
        orlnPrepStmt.setInt(1, ol_w_id);
        orlnPrepStmt.setInt(2, ol_d_id);
        orlnPrepStmt.setInt(3, ol_o_id);
        orlnPrepStmt.setInt(4, ol_number);
        orlnPrepStmt.setLong(5, ol_i_id);

        Timestamp delivery_d = new Timestamp(ol_delivery_d);
        orlnPrepStmt.setTimestamp(6, delivery_d);

        orlnPrepStmt.setDouble(7, ol_amount);
        orlnPrepStmt.setLong(8, ol_supply_w_id);
        orlnPrepStmt.setDouble(9, ol_quantity);
        orlnPrepStmt.setString(10, ol_dist_info); 

        orlnPrepStmt.addBatch();
    
      } catch(SQLException se) { 
        System.out.println(se.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
       }

    }  // end insertOrderLine()
    
    public void init() {
    	
    	numWarehouses = configWhseCount;
    	
    	initJDBC();
    	
    	// Create Tables
    	createTables();

        // Clearout the tables
        truncateTable("item");
        truncateTable("warehouse");
        truncateTable("stock");
        truncateTable("district");
        truncateTable("customer");
        truncateTable("history");
        truncateTable("oorder");
        truncateTable("order_line");
        truncateTable("new_order");
        
            //######################### MAINLINE ######################################
	    java.util.Date startDate = new java.util.Date();
      	System.out.println("------------- LoadData Start Date = " + startDate +
                       "-------------");

      	long startTimeMS = new java.util.Date().getTime();
      	long lastTimeMS = startTimeMS;

      	long totalRows = loadWhse(numWarehouses);
      	totalRows += loadItem(configItemCount);
      	totalRows += loadStock(numWarehouses, configItemCount);
      	totalRows += loadDist(numWarehouses, configDistPerWhse);
      	totalRows += loadCust(numWarehouses, configDistPerWhse, configCustPerDist);
      	totalRows += loadOrder(numWarehouses, configDistPerWhse, configCustPerDist);

      	long runTimeMS = (new java.util.Date().getTime()) + 1 - startTimeMS;
      	java.util.Date endDate = new java.util.Date();
      	System.out.println("");
      	System.out.println("------------- LoadJDBC Statistics --------------------");
      	System.out.println("     Start Time = " + startDate);
      	System.out.println("       End Time = " + endDate);
      	System.out.println("       Run Time = " + (int)runTimeMS/1000 + " Seconds");
      	System.out.println("    Rows Loaded = " + totalRows + " Rows");
      	System.out.println("Rows Per Second = "  + (totalRows/(runTimeMS/1000)) + " Rows/Sec");
      	System.out.println("------------------------------------------------------");

      	//exit Cleanly
      	try 
      	{
        	if (conn !=null)
             		conn.close();
	   	
      	} 
      	catch(SQLException se) {
       		se.printStackTrace();
     	 } // end try
  	
    } //end init
    
    public void tearDown() {
    	
    	try
    	{
    		if (this.conn != null) this.conn.close();
    	}
    	catch (SQLException se)
    	{
    		se.printStackTrace();
    	}
    	
    	
    } // end tearDown
	
	private void loadTransRollback () {
      
      	try {
          conn.rollback();
        } catch(SQLException se) {
          System.out.println(se.getMessage());
        }
	  
  }


  	private void loadTransCommit() {
	    try {
          conn.commit();
        } catch(SQLException se) {
          System.out.println(se.getMessage());
          loadTransRollback();
        }
	 }

	private void createTables() {
		
		try
		{
			Statement statement = conn.createStatement();

		    // sql query of string type to create a data base.
		    String QueryString = "CREATE TABLE WAREHOUSE" +
		    	      "(" +
        "W_NAME      CHAR(10)   NOT NULL," +
        "W_STREET_1  CHAR(20)   NOT NULL," +
        "W_STREET_2  CHAR(20)   NOT NULL," +
        "W_CITY      CHAR(20)   NOT NULL," +
        "W_STATE     CHAR(2)    NOT NULL," +
        "W_ZIP       CHAR(9)    NOT NULL," +
        "W_TAX       INTEGER    NOT NULL," +
        "W_YTD       BIGINT     NOT NULL," +
        "W_ID        INTEGER    NOT NULL," +
        "PRIMARY KEY (W_ID)" +
      "); " +
 		"CREATE TABLE DISTRICT" +
      "(" +
        "D_NEXT_O_ID INTEGER         NOT NULL," +
        "D_TAX       INTEGER         NOT NULL," +
        "D_YTD       BIGINT          NOT NULL," +
        "D_NAME      CHAR(10)        NOT NULL," +
        "D_STREET_1  CHAR(20)        NOT NULL," +
        "D_STREET_2  CHAR(20)        NOT NULL," +
        "D_CITY      CHAR(20)        NOT NULL," +
        "D_STATE     CHAR(2)         NOT NULL," +
        "D_ZIP       CHAR(9)         NOT NULL," +
        "D_ID        SMALLINT        NOT NULL," +
        "D_W_ID      INTEGER         NOT NULL," +
        "PRIMARY KEY (D_ID, D_W_ID)" +
      ");" +
 		"CREATE TABLE ITEM" +
      "(" +
        "I_NAME          CHAR(24)    NOT NULL," +
        "I_PRICE         INTEGER     NOT NULL," +
        "I_DATA          VARCHAR(50) NOT NULL," +
        "I_IM_ID         INTEGER     NOT NULL," +
        "I_ID            INTEGER     NOT NULL," +
        "PRIMARY KEY (I_ID)" +
      ");" +
		"CREATE TABLE STOCK" +
      "(" +
        "S_REMOTE_CNT    INTEGER     NOT NULL," +
        "S_QUANTITY      INTEGER     NOT NULL," +
        "S_ORDER_CNT     INTEGER     NOT NULL," +
        "S_YTD           INTEGER     NOT NULL," +
        "S_DATA          VARCHAR(50) NOT NULL," +
        "S_DIST_01       CHAR(24)    NOT NULL," +
        "S_DIST_02       CHAR(24)    NOT NULL," +
        "S_DIST_03       CHAR(24)    NOT NULL," +
        "S_DIST_04       CHAR(24)    NOT NULL," +
        "S_DIST_05       CHAR(24)    NOT NULL," +
        "S_DIST_06       CHAR(24)    NOT NULL," +
        "S_DIST_07       CHAR(24)    NOT NULL," +
        "S_DIST_08       CHAR(24)    NOT NULL," +
        "S_DIST_09       CHAR(24)    NOT NULL," +
        "S_DIST_10       CHAR(24)    NOT NULL," +
        "S_I_ID          INTEGER     NOT NULL," +
        "S_W_ID          INTEGER     NOT NULL," +
        "PRIMARY KEY (S_I_ID, S_W_ID)" +
      ");" +
	 	"CREATE TABLE CUSTOMER" +
      "(" +
        "C_ID            INTEGER       NOT NULL," +
        "C_STATE         CHAR(2)       NOT NULL," +
        "C_ZIP           CHAR(9)       NOT NULL," +
        "C_PHONE         CHAR(16)      NOT NULL," +
        "C_SINCE         DATETIME      NOT NULL," +
        "C_CREDIT_LIM    BIGINT        NOT NULL," +
        "C_MIDDLE        CHAR(2)       NOT NULL," +
        "C_CREDIT        CHAR(2)       NOT NULL," +
        "C_DISCOUNT      INTEGER       NOT NULL," +
        "C_DATA          VARCHAR(500)  NOT NULL," +
        "C_LAST          VARCHAR(16)   NOT NULL," +
        "C_FIRST         VARCHAR(16)   NOT NULL," +
        "C_STREET_1      VARCHAR(20)   NOT NULL," +
        "C_STREET_2      VARCHAR(20)   NOT NULL," +
        "C_CITY          VARCHAR(20)   NOT NULL," +
        "C_D_ID          SMALLINT      NOT NULL," +
        "C_W_ID          INTEGER       NOT NULL," +
        "C_DELIVERY_CNT  INTEGER       NOT NULL," +
        "C_BALANCE       BIGINT        NOT NULL," +
        "C_YTD_PAYMENT   BIGINT        NOT NULL," +
        "C_PAYMENT_CNT   INTEGER       NOT NULL," +
        "PRIMARY KEY (C_ID, C_D_ID, C_W_ID)" +
      ");" +
 		"CREATE INDEX CUST_IDXB " + 
   		"ON CUSTOMER (C_LAST, C_W_ID, C_D_ID, C_FIRST, C_ID);" +
		"CREATE TABLE HISTORY" +
      "(" +
        "H_C_ID          INTEGER     NOT NULL," +
        "H_C_D_ID        SMALLINT    NOT NULL," +
        "H_C_W_ID        INTEGER     NOT NULL," +
        "H_D_ID          SMALLINT    NOT NULL," +
        "H_W_ID          INTEGER     NOT NULL," +
        "H_DATE          DATETIME    NOT NULL," +
        "H_AMOUNT        INTEGER     NOT NULL," +
        "H_DATA          CHAR(24)    NOT NULL" +
      ");" +
 		"CREATE TABLE OORDER" +
      "(" +
        "O_C_ID          INTEGER     NOT NULL," +
        "O_ENTRY_D       DATETIME    NOT NULL," +
        "O_CARRIER_ID    SMALLINT," +
        "O_OL_CNT        SMALLINT    NOT NULL," +
        "O_ALL_LOCAL     SMALLINT    NOT NULL," +
        "O_ID            INTEGER     NOT NULL," +
        "O_W_ID          INTEGER     NOT NULL," +
        "O_D_ID          SMALLINT    NOT NULL," +
        "PRIMARY KEY (O_ID, O_W_ID, O_D_ID)" +
      ");" +
 		"CREATE INDEX ORDR_IDXB " +
      "ON OORDER (O_C_ID, O_W_ID, O_D_ID, O_ID DESC);" +
 		"CREATE TABLE ORDER_LINE" +
      "(" +
        "OL_DELIVERY_D    DATETIME," +
        "OL_AMOUNT        INTEGER    NOT NULL," +
        "OL_I_ID          INTEGER    NOT NULL," +
        "OL_SUPPLY_W_ID   INTEGER    NOT NULL," +
        "OL_QUANTITY      SMALLINT   NOT NULL," +
        "OL_DIST_INFO     CHAR(24)   NOT NULL," +
        "OL_O_ID          INTEGER    NOT NULL," +
        "OL_D_ID          SMALLINT   NOT NULL," +
        "OL_W_ID          INTEGER    NOT NULL," +
        "OL_NUMBER        SMALLINT   NOT NULL," +
        "PRIMARY KEY (OL_O_ID, OL_W_ID, OL_D_ID, OL_NUMBER)" +
      ");" +
		"CREATE TABLE NEW_ORDER" +
      "(" +
        "NO_O_ID         INTEGER     NOT NULL," +
        "NO_D_ID         SMALLINT    NOT NULL," +
        "NO_W_ID         INTEGER     NOT NULL," +
        "PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID)" +
      ");";

    		statement.execute(QueryString);
    		loadTransCommit();
    		System.out.println("Tables created");
    	}
    	catch (Exception e)
    	{
    		//e.printStackTrace();
    	}
		
	}

  private void truncateTable(String strTable) {

    System.out.println("Truncating '" + strTable + "' ...");
    try {
      stmt.execute("TRUNCATE TABLE " + strTable);
      loadTransCommit();
    } catch(SQLException se) {
      System.out.println(se.getMessage());
      loadTransRollback();
    }

  }



private void initJDBC() {

  try {
	/*
    // load the ini file
    Properties ini = new Properties();
    ini.load( new FileInputStream(System.getProperty("prop")));

    // display the values we need
    System.out.println("driver=" + ini.getProperty("driver"));
    System.out.println("conn=" + ini.getProperty("conn"));
    System.out.println("user=" + ini.getProperty("user"));
    System.out.println("password=******");

    // Register jdbcDriver
    Class.forName(ini.getProperty( "driver" ));
	*/
    // make connection
    //conn = DriverManager.getConnection(ini.getProperty("conn"),
    //  ini.getProperty("user"),ini.getProperty("password"));
    createTables();
    
    conn.setAutoCommit(false);

    // Create Statement
    stmt = conn.createStatement();

    distPrepStmt = conn.prepareStatement
      ("INSERT INTO district " +
       " (d_id, d_w_id, d_ytd, d_tax, d_next_o_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip) " +
       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    itemPrepStmt = conn.prepareStatement
      ("INSERT INTO item " +
       " (i_id, i_name, i_price, i_data, i_im_id) " +
       "VALUES (?, ?, ?, ?, ?)");

    custPrepStmt = conn.prepareStatement
      ("INSERT INTO customer " +
       " (c_id, c_d_id, c_w_id, " +
         "c_discount, c_credit, c_last, c_first, c_credit_lim, " +
         "c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, " +
         "c_street_1, c_street_2, c_city, c_state, c_zip, " +
         "c_phone, c_since, c_middle, c_data) " +
       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    histPrepStmt = conn.prepareStatement
      ("INSERT INTO history " +
       " (h_c_id, h_c_d_id, h_c_w_id, " +
         "h_d_id, h_w_id, " +
         "h_date, h_amount, h_data) " +
       "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

    ordrPrepStmt = conn.prepareStatement
      ("INSERT INTO oorder " +
       " (o_id, o_w_id,  o_d_id, o_c_id, " +
         "o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) " +
       "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

    orlnPrepStmt = conn.prepareStatement
      ("INSERT INTO order_line " +
       " (ol_w_id, ol_d_id, ol_o_id, " +
         "ol_number, ol_i_id, ol_delivery_d, " +
         "ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info) " +
       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    nworPrepStmt = conn.prepareStatement
      ("INSERT INTO new_order " +
       " (no_w_id, no_d_id, no_o_id) " +
       "VALUES (?, ?, ?)");

    stckPrepStmt = conn.prepareStatement
      ("INSERT INTO stock " +
       " (s_i_id, s_w_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, " +
         "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
         "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10) " +
       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    whsePrepStmt = conn.prepareStatement
       ("INSERT INTO warehouse " +
        " (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

  } catch(SQLException se) {
    System.out.println(se.getMessage());
    loadTransRollback();

  } catch(Exception e) {
    e.printStackTrace();
    loadTransRollback();

  }  // end try

} // end initJDBC()


  private int loadItem(int itemKount) {

      int k = 0;
      int t = 0;
      int randPct = 0;
      int len = 0;
      int startORIGINAL = 0;

      try {

        now = new java.util.Date();
        t = itemKount;
        System.out.println("\nStart Item Load for " + t + " Items @ " + now + " ...");

        if (outputFiles == true)
        {
            out = new PrintWriter(new FileOutputStream(fileLocation + "csv"));
            System.out.println("\nWriting Item file to: " + fileLocation + "csv");
        }

        //Item item  = new Item();

        for (int i=1; i <= itemKount; i++) {

          int i_id = i;
          String i_name = randomStr(randomNumber(14,24,gen));
          String i_data;
          float i_price = (float)(randomNumber(100,10000,gen)/100.0);

          // i_data
          randPct = randomNumber(1, 100, gen);
          len = randomNumber(26, 50, gen);
          if ( randPct > 10 ) {
             // 90% of time i_data isa random string of length [26 .. 50]
             i_data = randomStr(len);
          } else {
            // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
            startORIGINAL = randomNumber(2, (len - 8), gen);
            i_data =
              randomStr(startORIGINAL - 1) +
              "ORIGINAL" +
              randomStr(len - startORIGINAL - 9);
          }

          int i_im_id = randomNumber(1, 10000, gen);

          k++;

          if (outputFiles == false)
          {
            itemPrepStmt.setLong(1, i_id);
            itemPrepStmt.setString(2, i_name);
            itemPrepStmt.setDouble(3, i_price);
            itemPrepStmt.setString(4, i_data);
            itemPrepStmt.setLong(5, i_im_id);
            itemPrepStmt.addBatch();

            if (( k % configCommitCount) == 0) {
              long tmpTime = new java.util.Date().getTime();
              String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
              System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
              lastTimeMS = tmpTime;
              itemPrepStmt.executeBatch();
              itemPrepStmt.clearBatch();
              loadTransCommit();
            }
	      } else {
			String str = "";
            str = str + i_id + ",";
            str = str + i_name + ",";
            str = str + i_price + ",";
            str = str + i_data + ",";
            str = str + i_im_id;
    		out.println(str);

            if (( k % configCommitCount) == 0) {
              long tmpTime = new java.util.Date().getTime();
              String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
              System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
              lastTimeMS = tmpTime;
		    }
		  }

        } // end for

        long tmpTime = new java.util.Date().getTime();
        String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
        System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
        lastTimeMS = tmpTime;

        if (outputFiles == false)
        {
          itemPrepStmt.executeBatch();
	    }
        loadTransCommit();
        now = new java.util.Date();
        System.out.println("End Item Load @  " + now);

      } catch(SQLException se) {
        System.out.println(se.getMessage());
        loadTransRollback();
      } catch(Exception e) {
        e.printStackTrace();
        loadTransRollback();
      }

      return(k);

} // end loadItem()



  private int loadWhse(int whseKount) {

      try {

        now = new java.util.Date();
        System.out.println("\nStart Whse Load for " + whseKount + " Whses @ " + now + " ...");

        if (outputFiles == true)
        {
            out = new PrintWriter(new FileOutputStream(fileLocation + "csv"));
            System.out.println("\nWriting Warehouse file to: " + fileLocation + "csv");
        }

        //Warehouse warehouse  = new Warehouse();
        for (int i=1; i <= whseKount; i++) {

          int w_id       = i;
          float w_ytd      = 300000;

          // random within [0.0000 .. 0.2000]
          float w_tax = (float)((randomNumber(0,2000,gen))/10000.0);

          String w_name     = randomStr(randomNumber(6,10,gen));
          String w_street_1 = randomStr(randomNumber(10,20,gen));
          String w_street_2 = randomStr(randomNumber(10,20,gen));
          String w_city     = randomStr(randomNumber(10,20,gen));
          String w_state    = randomStr(3).toUpperCase();
          String w_zip      = "123456789";

          if (outputFiles == false)
          {
            whsePrepStmt.setLong(1, w_id);
            whsePrepStmt.setDouble(2, w_ytd);
            whsePrepStmt.setDouble(3, w_tax);
            whsePrepStmt.setString(4, w_name);
            whsePrepStmt.setString(5, w_street_1);
            whsePrepStmt.setString(6, w_street_2);
            whsePrepStmt.setString(7, w_city);
            whsePrepStmt.setString(8, w_state);
            whsePrepStmt.setString(9, w_zip);
            whsePrepStmt.executeUpdate();
	      } else {
			String str = "";
            str = str + w_id + ",";
            str = str + w_ytd + ",";
            str = str + w_tax + ",";
            str = str + w_name + ",";
            str = str + w_street_1 + ",";
            str = str + w_street_2 + ",";
            str = str + w_city + ",";
            str = str + w_state + ",";
            str = str + w_zip;
    		out.println(str);
	      }

        } // end for

        loadTransCommit();
        now = new java.util.Date();

        long tmpTime = new java.util.Date().getTime();
        System.out.println("Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000));
        lastTimeMS = tmpTime;
        System.out.println("End Whse Load @  " + now);

      } catch(SQLException se) {
        System.out.println(se.getMessage());
        loadTransRollback();
      } catch(Exception e) {
        e.printStackTrace();
        loadTransRollback();
      }

      return (whseKount);

  } // end loadWhse()



  private int loadStock(int whseKount, int itemKount) {

      int k = 0;
      int t = 0;
      int randPct = 0;
      int len = 0;
      int startORIGINAL = 0;

      try {

        now = new java.util.Date();
        t = (whseKount * itemKount);
        System.out.println("\nStart Stock Load for " + t + " units @ " + now + " ...");

        if (outputFiles == true)
        {
            out = new PrintWriter(new FileOutputStream(fileLocation + "csv"));
            System.out.println("\nWriting Stock file to: " + fileLocation + "csv");
        }

        // Stock stock  = new Stock();

        for (int i=1; i <= itemKount; i++) {

          for (int w=1; w <= whseKount; w++) {

            int s_i_id = i;
            int s_w_id = w;
            int s_quantity = randomNumber(10, 100, gen);
            float s_ytd = 0;
            int s_order_cnt = 0;
            int s_remote_cnt = 0;
            String s_data;

            // s_data
            randPct = randomNumber(1, 100, gen);
            len = randomNumber(26, 50, gen);
            if ( randPct > 10 ) {
               // 90% of time i_data isa random string of length [26 .. 50]
               s_data = randomStr(len);
            } else {
              // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
              startORIGINAL = randomNumber(2, (len - 8), gen);
              s_data =
                randomStr(startORIGINAL - 1) +
                "ORIGINAL" +
                randomStr(len - startORIGINAL - 9);
            }

            String s_dist_01 = randomStr(24);
            String s_dist_02 = randomStr(24);
            String s_dist_03 = randomStr(24);
            String s_dist_04 = randomStr(24);
            String s_dist_05 = randomStr(24);
            String s_dist_06 = randomStr(24);
            String s_dist_07 = randomStr(24);
            String s_dist_08 = randomStr(24);
            String s_dist_09 = randomStr(24);
            String s_dist_10 = randomStr(24);

          k++;
          if (outputFiles == false)
          {
              stckPrepStmt.setLong(1, s_i_id);
              stckPrepStmt.setLong(2, s_w_id);
              stckPrepStmt.setDouble(3, s_quantity);
              stckPrepStmt.setDouble(4, s_ytd);
              stckPrepStmt.setLong(5, s_order_cnt);
              stckPrepStmt.setLong(6, s_remote_cnt);
              stckPrepStmt.setString(7, s_data);
              stckPrepStmt.setString(8, s_dist_01);
              stckPrepStmt.setString(9, s_dist_02);
              stckPrepStmt.setString(10, s_dist_03);
              stckPrepStmt.setString(11, s_dist_04);
              stckPrepStmt.setString(12, s_dist_05);
              stckPrepStmt.setString(13, s_dist_06);
              stckPrepStmt.setString(14, s_dist_07);
              stckPrepStmt.setString(15, s_dist_08);
              stckPrepStmt.setString(16, s_dist_09);
              stckPrepStmt.setString(17, s_dist_10);
              stckPrepStmt.addBatch();
            if (( k % configCommitCount) == 0) {
              long tmpTime = new java.util.Date().getTime();
              String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
              System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
              lastTimeMS = tmpTime;
              stckPrepStmt.executeBatch();
              stckPrepStmt.clearBatch();
              loadTransCommit();
            }
	      } else {
			String str = "";
              str = str + s_i_id + ",";
              str = str + s_w_id + ",";
              str = str + s_quantity + ",";
              str = str + s_ytd + ",";
              str = str + s_order_cnt + ",";
              str = str + s_remote_cnt + ",";
              str = str + s_data + ",";
              str = str + s_dist_01 + ",";
              str = str + s_dist_02 + ",";
              str = str + s_dist_03 + ",";
              str = str + s_dist_04 + ",";
              str = str + s_dist_05 + ",";
              str = str + s_dist_06 + ",";
              str = str + s_dist_07 + ",";
              str = str + s_dist_08 + ",";
              str = str + s_dist_09 + ",";
              str = str + s_dist_10;
              out.println(str);

            if (( k % configCommitCount) == 0) {
              long tmpTime = new java.util.Date().getTime();
              String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
              System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
              lastTimeMS = tmpTime;
		      }
           }

          } // end for [w]

        } // end for [i]


        long tmpTime = new java.util.Date().getTime();
        String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
        System.out.println(etStr.substring(0, 30) + "  Writing final records " + k + " of " + t);
        lastTimeMS = tmpTime;
        if (outputFiles == false)
        {
          stckPrepStmt.executeBatch();
	    }
        loadTransCommit();

        now = new java.util.Date();
        System.out.println("End Stock Load @  " + now);

      } catch(SQLException se) {
        System.out.println(se.getMessage());
        loadTransRollback();

      } catch(Exception e) {
        e.printStackTrace();
        loadTransRollback();
      }

      return (k);

  } // end loadStock()



  private int loadDist(int whseKount, int distWhseKount) {

      int k = 0;
      int t = 0;

      try {

        now = new java.util.Date();

        if (outputFiles == true)
        {
            out = new PrintWriter(new FileOutputStream(fileLocation + "csv"));
            System.out.println("\nWriting District file to: " + fileLocation + "csv");
        }

        //District district  = new District();

        t = (whseKount * distWhseKount);
        System.out.println("\nStart District Data for " + t + " Dists @ " + now + " ...");

        for (int w=1; w <= whseKount; w++) {

          for (int d=1; d <= distWhseKount; d++) {

            int d_id = d;
            int d_w_id = w;
            float d_ytd = 30000;

            // random within [0.0000 .. 0.2000]
            float d_tax = (float)((randomNumber(0,2000,gen))/10000.0);

            int d_next_o_id = 3001;
            String d_name = randomStr(randomNumber(6,10,gen));
            String d_street_1 = randomStr(randomNumber(10,20,gen));
            String d_street_2 = randomStr(randomNumber(10,20,gen));
            String d_city = randomStr(randomNumber(10,20,gen));
            String d_state = randomStr(3).toUpperCase();
            String d_zip = "123456789";

          k++;
          if (outputFiles == false)
          {
              distPrepStmt.setLong(1, d_id);
              distPrepStmt.setLong(2, d_w_id);
              distPrepStmt.setDouble(3, d_ytd);
              distPrepStmt.setDouble(4, d_tax);
              distPrepStmt.setLong(5, d_next_o_id);
              distPrepStmt.setString(6, d_name);
              distPrepStmt.setString(7, d_street_1);
              distPrepStmt.setString(8, d_street_2);
              distPrepStmt.setString(9, d_city);
              distPrepStmt.setString(10, d_state);
              distPrepStmt.setString(11, d_zip);
              distPrepStmt.executeUpdate();
	      } else {
              String str = "";
              str = str + d_id + ",";
              str = str + d_w_id + ",";
              str = str + d_ytd + ",";
              str = str + d_tax + ",";
              str = str + d_next_o_id + ",";
              str = str + d_name + ",";
              str = str + d_street_1 + ",";
              str = str + d_street_2 + ",";
              str = str + d_city + ",";
              str = str + d_state + ",";
              str = str + d_zip;
              out.println(str);
          }

          } // end for [d]

        } // end for [w]

        long tmpTime = new java.util.Date().getTime();
        String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
        System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
        lastTimeMS = tmpTime;
        loadTransCommit();
        now = new java.util.Date();
        System.out.println("End District Load @  " + now);

      } catch(SQLException se) {
        System.out.println(se.getMessage());
        loadTransRollback();
      } catch(Exception e) {
        e.printStackTrace();
        loadTransRollback();
      }

      return (k);

  } // end loadDist()



  private int loadCust(int whseKount, int distWhseKount, int custDistKount) {

      int k = 0;
      int t = 0;
      double cCreditLim = 0;

      //Customer customer  = new Customer();
      //History history = new History();
      PrintWriter outHist = null;


      try {

        now = new java.util.Date();

        if (outputFiles == true)
        {
            out = new PrintWriter(new FileOutputStream(fileLocation + "csv"));
            System.out.println("\nWriting Customer file to: " + fileLocation + "csv");
            outHist = new PrintWriter(new FileOutputStream(fileLocation + "cust-hist.csv"));
            System.out.println("\nWriting Customer History file to: " + fileLocation + "cust-hist.csv");
        }

        t = (whseKount * distWhseKount * custDistKount * 2);
        System.out.println("\nStart Cust-Hist Load for " +
           t + " Cust-Hists @ " + now + " ...");

        for (int w=1; w <= whseKount; w++) {

          for (int d=1; d <= distWhseKount; d++) {

            for (int c=1; c <= custDistKount; c++) {

              sysdate = new java.sql.Timestamp(System.currentTimeMillis());

              int c_id =  c;
              int c_d_id = d;
              int c_w_id =  w;
              String c_credit;
			
              // discount is random between [0.0000 ... 0.5000]
              float c_discount =
                (float)(randomNumber(1,5000,gen) / 10000.0);

              if (randomNumber(1,100,gen) <= 10) {
                c_credit =  "BC";   // 10% Bad Credit
              } else {
                c_credit =  "GC";   // 90% Good Credit
              }
              String c_last =  getLastName(gen);
              String c_first =  randomStr(randomNumber(8,16,gen));
              float c_credit_lim =  50000;

              float c_balance =  -10;
              float c_ytd_payment =  10;
              int c_payment_cnt =  1;
              int c_delivery_cnt =  0;

              String c_street_1 =  randomStr(randomNumber(10,20,gen));
              String c_street_2 =  randomStr(randomNumber(10,20,gen));
              String c_city =  randomStr(randomNumber(10,20,gen));
              String c_state =  randomStr(3).toUpperCase();
              String c_zip =  "123456789";

              String c_phone =  "(732)744-1700";

              long c_since =  sysdate.getTime();
              String c_middle =  "OE";
              String c_data = randomStr(randomNumber(300,500,gen));

              int h_c_id = c;
              int h_c_d_id = d;
              int h_c_w_id = w;
              int h_d_id = d;
              int h_w_id = w;
              long h_date = sysdate.getTime();
              float h_amount = 10;
              String h_data =  randomStr(randomNumber(10,24,gen));

              k = k + 2;
              if (outputFiles == false)
              {
                custPrepStmt.setInt(1, c_id);
                custPrepStmt.setInt(2, c_d_id);
                custPrepStmt.setInt(3, c_w_id);
                custPrepStmt.setDouble(4, c_discount);
                custPrepStmt.setString(5, c_credit);
                custPrepStmt.setString(6, c_last);
                custPrepStmt.setString(7, c_first);
                custPrepStmt.setDouble(8, c_credit_lim);
                custPrepStmt.setDouble(9, c_balance);
                custPrepStmt.setDouble(10, c_ytd_payment);
                custPrepStmt.setDouble(11, c_payment_cnt);
                custPrepStmt.setDouble(12, c_delivery_cnt);
                custPrepStmt.setString(13, c_street_1);
                custPrepStmt.setString(14, c_street_2);
                custPrepStmt.setString(15, c_city);
                custPrepStmt.setString(16, c_state);
                custPrepStmt.setString(17, c_zip);
                custPrepStmt.setString(18, c_phone);

                Timestamp since = new Timestamp(c_since);
                custPrepStmt.setTimestamp(19, since);
                custPrepStmt.setString(20, c_middle);
                custPrepStmt.setString(21, c_data);

                custPrepStmt.addBatch();

                histPrepStmt.setInt(1, h_c_id);
                histPrepStmt.setInt(2, h_c_d_id);
                histPrepStmt.setInt(3, h_c_w_id);

                histPrepStmt.setInt(4, h_d_id);
                histPrepStmt.setInt(5, h_w_id);
                Timestamp hdate = new Timestamp(h_date);
                histPrepStmt.setTimestamp(6, hdate);
                histPrepStmt.setDouble(7, h_amount);
                histPrepStmt.setString(8, h_data);

                histPrepStmt.addBatch();


              if (( k % configCommitCount) == 0) {
                long tmpTime = new java.util.Date().getTime();
                String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
                System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
                lastTimeMS = tmpTime;

                custPrepStmt.executeBatch();
                histPrepStmt.executeBatch();
                custPrepStmt.clearBatch();
                custPrepStmt.clearBatch();
                loadTransCommit();
              }
           } else {
	          String str = "";
              str = str + c_id + ",";
              str = str + c_d_id + ",";
              str = str + c_w_id + ",";
              str = str + c_discount + ",";
              str = str + c_credit + ",";
              str = str + c_last + ",";
              str = str + c_first + ",";
              str = str + c_credit_lim + ",";
              str = str + c_balance + ",";
              str = str + c_ytd_payment + ",";
              str = str + c_payment_cnt + ",";
              str = str + c_delivery_cnt + ",";
              str = str + c_street_1 + ",";
              str = str + c_street_2 + ",";
              str = str + c_city + ",";
              str = str + c_state + ",";
              str = str + c_zip + ",";
              str = str + c_phone;
              out.println(str);

              str = "";
              str = str + h_c_id + ",";
              str = str + h_c_d_id + ",";
              str = str + h_c_w_id + ",";
              str = str + h_d_id + ",";
              str = str + h_w_id + ",";
              Timestamp hdate = new Timestamp(h_date);
              str = str + hdate + ",";
              str = str + h_amount + ",";
              str = str + h_data;
              outHist.println(str);

              if (( k % configCommitCount) == 0) {
                long tmpTime = new java.util.Date().getTime();
                String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
                System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
                lastTimeMS = tmpTime;

		        }
           }

            } // end for [c]

          } // end for [d]

        } // end for [w]


        long tmpTime = new java.util.Date().getTime();
        String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
        System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
        lastTimeMS = tmpTime;
        custPrepStmt.executeBatch();
        histPrepStmt.executeBatch();
        loadTransCommit();
        now = new java.util.Date();
        if (outputFiles == true)
        {
          outHist.close();
	    }
        System.out.println("End Cust-Hist Data Load @  " + now);

      } catch(SQLException se) {
        System.out.println(se.getMessage());
        loadTransRollback();
        if (outputFiles == true)
        {
          outHist.close();
	    }
      } catch(Exception e) {
        e.printStackTrace();
        loadTransRollback();
        if (outputFiles == true)
        {
          outHist.close();
	    }
      }

      return(k);

  } // end loadCust()



  private int loadOrder(int whseKount, int distWhseKount, int custDistKount) {

      int k     = 0;
      int t     = 0;
      PrintWriter outLine     = null;
      PrintWriter outNewOrder = null;

      try {

        if (outputFiles == true)
        {
            out = new PrintWriter(new FileOutputStream(fileLocation + "order.csv"));
            System.out.println("\nWriting Order file to: " + fileLocation + "order.csv");
            outLine = new PrintWriter(new FileOutputStream(fileLocation + "order-line.csv"));
            System.out.println("\nWriting Order Line file to: " + fileLocation + "order-line.csv");
            outNewOrder = new PrintWriter(new FileOutputStream(fileLocation + "new-order.csv"));
            System.out.println("\nWriting New Order file to: " + fileLocation + "new-order.csv");
        }

        now = new java.util.Date();
        //Oorder oorder  = new Oorder();
        //NewOrder new_order  = new NewOrder();
        //OrderLine order_line  = new OrderLine();
        //jdbcIO myJdbcIO = new jdbcIO();

        t = (whseKount * distWhseKount * custDistKount);
        t = (t * 11) + (t / 3);
        System.out.println("whse=" + whseKount +", dist=" + distWhseKount +
           ", cust=" + custDistKount);
        System.out.println("\nStart Order-Line-New Load for approx " +
           t  + " rows @ " + now + " ...");

        for (int w=1; w <= whseKount; w++) {

          for (int d=1; d <= distWhseKount; d++) {

            for (int c=1; c <= custDistKount; c++) {

              int o_id = c;
              int o_w_id = w;
              int o_d_id = d;
              int o_c_id = randomNumber(1, custDistKount, gen);
              int o_carrier_id = randomNumber(1, 10, gen);
              int o_ol_cnt = randomNumber(5, 15, gen);
              int o_all_local = 1;
              long o_entry_d = System.currentTimeMillis();
              
              int no_w_id;
              int no_d_id;
              int no_o_id;
              
              int ol_w_id;
              int ol_d_id;
              int ol_o_id;
              int ol_number;
              int ol_i_id;
              int ol_supply_w_id;
              int ol_quantity;
              long ol_delivery_d;
              float ol_amount;
              String ol_dist_info;
              
              k++;
              if (outputFiles == false)
              {
              		insertOrder(ordrPrepStmt, o_id, o_w_id, o_d_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d);
    	      } 
    	      else 
    	      {
	    			String str = "";
	                str = str + o_id + ",";
	                str = str + o_w_id + ",";
	                str = str + o_d_id + ",";
	                str = str + o_c_id + ",";
	                str = str + o_carrier_id + ",";
	                str = str + o_ol_cnt + ",";
	                str = str + o_all_local + ",";
	                Timestamp entry_d = new java.sql.Timestamp(o_entry_d);
	                str = str + entry_d;
	                out.println(str);
              }

              // 900 rows in the NEW-ORDER table corresponding to the last
              // 900 rows in the ORDER table for that district (i.e., with
              // NO_O_ID between 2,101 and 3,000)

              if (c > 2100 ) {

                no_w_id = w;
                no_d_id = d;
                no_o_id = c;

                k++;
                if (outputFiles == false)
                {
                  insertNewOrder(nworPrepStmt, no_w_id, no_d_id, no_o_id);
                } else {
                  String str = "";
                  str = str + no_w_id+ ",";
                  str = str + no_d_id+ ",";
                  str = str + no_o_id;
                  outNewOrder.println(str);
                }


              } // end new order

              for (int l=1; l <= o_ol_cnt; l++) {

                ol_w_id = w;
                ol_d_id = d;
                ol_o_id = c;
                ol_number =  l;   // ol_number
                ol_i_id =  randomNumber(1, 100000, gen);
                ol_delivery_d =  o_entry_d;

                if (ol_o_id < 2101) {
                  ol_amount = 0;
                } else {
                  // random within [0.01 .. 9,999.99]
                  ol_amount =
                    (float)(randomNumber(1, 999999, gen) / 100.0);
                }

                ol_supply_w_id =  randomNumber(1, numWarehouses, gen);
                ol_quantity =  5;
                ol_dist_info =  randomStr(24);

                k++;
                if (outputFiles == false)
                {
                  insertOrderLine(orlnPrepStmt, ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount,	ol_supply_w_id, ol_quantity, ol_dist_info);
                } else {
                  String str = "";
                  str = str + ol_w_id + ",";
                  str = str + ol_d_id + ",";
                  str = str + ol_o_id + ",";
                  str = str + ol_number + ",";
                  str = str + ol_i_id + ",";
                  Timestamp delivery_d = new Timestamp(ol_delivery_d);
                  str = str + delivery_d + ",";
                  str = str + ol_amount + ",";
                  str = str + ol_supply_w_id + ",";
                  str = str + ol_quantity + ",";
                  str = str + ol_dist_info;
                  outLine.println(str);
                }

                if (( k % configCommitCount) == 0) {
                  long tmpTime = new java.util.Date().getTime();
                  String etStr = "  Elasped Time(ms): " + ((tmpTime - lastTimeMS)/1000.000) + "                    ";
                  System.out.println(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
                  lastTimeMS = tmpTime;
                  if (outputFiles == false)
                  {
                    ordrPrepStmt.executeBatch();
                    nworPrepStmt.executeBatch();
                    orlnPrepStmt.executeBatch();
                    ordrPrepStmt.clearBatch();
                    nworPrepStmt.clearBatch();
                    orlnPrepStmt.clearBatch();
                    loadTransCommit();
				  }
                }

              } // end for [l]

            } // end for [c]

          } // end for [d]

        } // end for [w]


        System.out.println("  Writing final records " + k + " of " + t);
        if (outputFiles == false)
        {
          ordrPrepStmt.executeBatch();
          nworPrepStmt.executeBatch();
          orlnPrepStmt.executeBatch();
	    } else {
          outLine.close();
          outNewOrder.close();
		}
        loadTransCommit();
        now = new java.util.Date();
        System.out.println("End Orders Load @  " + now);

      } catch(Exception e) {
        e.printStackTrace();
        loadTransRollback();
        if (outputFiles == true)
        {
          outLine.close();
          outNewOrder.close();
	    }
      }

      return(k);

  } // end loadOrder()

	
	
	public boolean TPCC_NewOrderTransaction()
    {
    	
    	try
	    {
	    	if (conn.isClosed())
    		{
	       		Class.forName(driver);
				this.conn=DriverManager.getConnection(url,"sa","1234");
				conn.setAutoCommit(false);
	    	}
	    }
    	catch (Exception e)
    	{
    		e.printStackTrace();
    	}
		boolean complete = true;    	
    	
    	gen = new Random(System.currentTimeMillis());  	
    	
    	int w_id = randomNumber(1, configWhseCount, gen);
    	int d_id = randomNumber(1, configDistPerWhse, gen);
        int c_id = getCustomerID(gen);

        int o_ol_cnt = (int)randomNumber(5, 15, gen);
        int[] itemIDs = new int[o_ol_cnt];
        int[] supplierWarehouseIDs = new int[o_ol_cnt];
        int[] orderQuantities = new int[o_ol_cnt];
        int o_all_local = 1;
        for(int i = 0; i < o_ol_cnt; i++)
        {
            itemIDs[i] = getItemID(gen);
            if(randomNumber(1, 100, gen) > 1)
            {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            }
            else
            {
                do
                {
                    supplierWarehouseIDs[i] = randomNumber(1, configWhseCount, gen);
                }
                while(supplierWarehouseIDs[i] == terminalWarehouseID && numWarehouses > 1);
                o_all_local = 0;
            }
            orderQuantities[i] = randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if(randomNumber(1, 100, gen) == 1)
            itemIDs[o_ol_cnt-1] = -12345;

        float c_discount, w_tax, d_tax = 0, i_price;
        int d_next_o_id, o_id = -1, s_quantity;
        String c_last = null, c_credit = null, i_name, i_data, s_data;
        String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
        String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
        float[] itemPrices = new float[o_ol_cnt];
        float[] orderLineAmounts = new float[o_ol_cnt];
        String[] itemNames = new String[o_ol_cnt];
        int[] stockQuantities = new int[o_ol_cnt];
        char[] brandGeneric = new char[o_ol_cnt];
        int ol_supply_w_id, ol_i_id, ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount, total_amount = 0;
        boolean newOrderRowInserted;
		
		/*
        Warehouse whse = new Warehouse();
        Customer  cust = new Customer();
        District  dist = new District();
        NewOrder  nwor = new NewOrder();
        Oorder    ordr = new Oorder();
        OrderLine orln = new OrderLine();
        Stock     stck = new Stock();
        Item      item = new Item();
		*/
        try {

            if (stmtGetCustWhse == null) {
              stmtGetCustWhse = conn.prepareStatement(
                "SELECT c_discount, c_last, c_credit, w_tax" +
                "  FROM customer, warehouse" +
                " WHERE w_id = ? AND w_id = c_w_id" +
                  " AND c_d_id = ? AND c_id = ?");
            }
            stmtGetCustWhse.setInt(1, w_id);
            stmtGetCustWhse.setInt(2, d_id);
            stmtGetCustWhse.setInt(3, c_id);
            //rs = stmtGetCustWhse.executeQuery();
            //if(!rs.next()) throw new Exception("W_ID=" + w_id + " C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
            c_discount = rs.getFloat("c_discount");
            c_last = rs.getString("c_last");
            c_credit = rs.getString("c_credit");
            w_tax = rs.getFloat("w_tax");
            //rs.close();
            //rs = null;

            newOrderRowInserted = false;
            while(!newOrderRowInserted)
            {

                if (stmtGetDist == null) {
                  stmtGetDist = conn.prepareStatement(
                    "SELECT d_next_o_id, d_tax FROM district" +
                    " WHERE d_id = ? AND d_w_id = ?");// FOR UPDATE"); <-- was original (its Oracle-specific)
                }
                stmtGetDist.setInt(1, d_id);
                stmtGetDist.setInt(2, w_id);
                //rs = stmtGetDist.executeQuery();
                //if(!rs.next()) throw new Exception("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                d_next_o_id = rs.getInt("d_next_o_id");
                d_tax = rs.getFloat("d_tax");
                //rs.close();
                //rs = null;
                o_id = d_next_o_id;

                try
                {
                    if (stmtInsertNewOrder == null) {
                      stmtInsertNewOrder = conn.prepareStatement(
                        "INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) " +
                        "VALUES ( ?, ?, ?)");
                    }
                    stmtInsertNewOrder.setInt(1, o_id);
                    stmtInsertNewOrder.setInt(2, d_id);
                    stmtInsertNewOrder.setInt(3, w_id);
                    //stmtInsertNewOrder.executeUpdate();
                    newOrderRowInserted = true;
                }
                catch(SQLException e2)
                {
                    //printMessage("The row was already on table  Restarting...");
                }
            }


            if (stmtUpdateDist == null) {
              stmtUpdateDist = conn.prepareStatement(
                "UPDATE district SET d_next_o_id = d_next_o_id + 1 " +
                " WHERE d_id = ? AND d_w_id = ?");
            }
            stmtUpdateDist.setInt(1,d_id);
            stmtUpdateDist.setInt(2,w_id);
            //result = stmtUpdateDist.executeUpdate();
            //if(result == 0) throw new Exception("Error!! Cannot update next_order_id on DISTRICT for D_ID=" + d_id + " D_W_ID=" + w_id);

              if (stmtInsertOOrder == null) {
                stmtInsertOOrder = conn.prepareStatement(
                  "INSERT INTO OORDER " +
                  " (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)" +
                  " VALUES (?, ?, ?, ?, ?, ?, ?)");
              }
              stmtInsertOOrder.setInt(1,o_id);
              stmtInsertOOrder.setInt(2,d_id);
              stmtInsertOOrder.setInt(3,w_id);
              stmtInsertOOrder.setInt(4,c_id);
              stmtInsertOOrder.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
              stmtInsertOOrder.setInt(6,o_ol_cnt);
              stmtInsertOOrder.setInt(7,o_all_local);
              //stmtInsertOOrder.executeUpdate();

            for(int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supplierWarehouseIDs[ol_number-1];
                ol_i_id = itemIDs[ol_number-1];
                ol_quantity = orderQuantities[ol_number-1];

                if (ol_i_id == -12345) {
                  // an expected condition generated 1% of the time in the test data...
                  // we throw an illegal access exception and the transaction gets rolled back later on
                  throw new IllegalAccessException("Expected NEW-ORDER error condition excersing rollback functionality");
                }

                  if (stmtGetItem == null) {
                    stmtGetItem = conn.prepareStatement(
                      "SELECT i_price, i_name , i_data FROM item WHERE i_id = ?");
                  }
                  stmtGetItem.setInt(1, ol_i_id);
                  //rs = stmtGetItem.executeQuery();
                  //if(!rs.next()) throw new IllegalAccessException("I_ID=" + ol_i_id + " not found!");
                  i_price = rs.getFloat("i_price");
                  i_name = rs.getString("i_name");
                  i_data = rs.getString("i_data");
                  //rs.close();
                  //rs = null;

                itemPrices[ol_number-1] = i_price;
                itemNames[ol_number-1] = i_name;

                if (stmtGetStock == null) {
                  stmtGetStock = conn.prepareStatement(
                      "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
                      "       s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10" +
                      " FROM stock WHERE s_i_id = ? AND s_w_id = ?"); // FOR UPDATE"); again Oracle-specific
                }
                stmtGetStock.setInt(1, ol_i_id);
                stmtGetStock.setInt(2, ol_supply_w_id);
                //rs = stmtGetStock.executeQuery();
                //if(!rs.next()) throw new Exception(ol_supply_w_id + " I_ID=" + ol_i_id + " not found!");
                s_quantity = rs.getInt("s_quantity");
                s_data = rs.getString("s_data");
                s_dist_01 = rs.getString("s_dist_01");
                s_dist_02 = rs.getString("s_dist_02");
                s_dist_03 = rs.getString("s_dist_03");
                s_dist_04 = rs.getString("s_dist_04");
                s_dist_05 = rs.getString("s_dist_05");
                s_dist_06 = rs.getString("s_dist_06");
                s_dist_07 = rs.getString("s_dist_07");
                s_dist_08 = rs.getString("s_dist_08");
                s_dist_09 = rs.getString("s_dist_09");
                s_dist_10 = rs.getString("s_dist_10");
                //rs.close();
                //rs = null;

                stockQuantities[ol_number-1] = s_quantity;

                if(s_quantity - ol_quantity >= 10) {
                    s_quantity -= ol_quantity;
                } else {
                    s_quantity += -ol_quantity + 91;
                }

                if(ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }


                if (stmtUpdateStock == null) {
                  stmtUpdateStock = conn.prepareStatement(
                    "UPDATE stock SET s_quantity = ? , s_ytd = s_ytd + ?, s_remote_cnt = s_remote_cnt + ? " +
                    " WHERE s_i_id = ? AND s_w_id = ?");
                }
                stmtUpdateStock.setInt(1,s_quantity);
                stmtUpdateStock.setInt(2, ol_quantity);
                stmtUpdateStock.setInt(3,s_remote_cnt_increment);
                stmtUpdateStock.setInt(4,ol_i_id);
                stmtUpdateStock.setInt(5,ol_supply_w_id);
                stmtUpdateStock.addBatch();

                ol_amount = ol_quantity * i_price;
                orderLineAmounts[ol_number-1] = ol_amount;
                total_amount += ol_amount;

                if(i_data.indexOf("GENERIC") != -1 && s_data.indexOf("GENERIC") != -1) {
                    brandGeneric[ol_number-1] = 'B';
                } else {
                    brandGeneric[ol_number-1] = 'G';
                }

                switch((int)d_id) {
                    case 1: ol_dist_info = s_dist_01; break;
                    case 2: ol_dist_info = s_dist_02; break;
                    case 3: ol_dist_info = s_dist_03; break;
                    case 4: ol_dist_info = s_dist_04; break;
                    case 5: ol_dist_info = s_dist_05; break;
                    case 6: ol_dist_info = s_dist_06; break;
                    case 7: ol_dist_info = s_dist_07; break;
                    case 8: ol_dist_info = s_dist_08; break;
                    case 9: ol_dist_info = s_dist_09; break;
                    case 10: ol_dist_info = s_dist_10; break;
                }

                  if (stmtInsertOrderLine == null) {
                    stmtInsertOrderLine = conn.prepareStatement(
                      "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id," +
                      "  ol_quantity, ol_amount, ol_dist_info) VALUES (?,?,?,?,?,?,?,?,?)");
                  }
                  stmtInsertOrderLine.setInt(1, o_id);
                  stmtInsertOrderLine.setInt(2, d_id);
                  stmtInsertOrderLine.setInt(3, w_id);
                  stmtInsertOrderLine.setInt(4, ol_number);
                  stmtInsertOrderLine.setInt(5, ol_i_id);
                  stmtInsertOrderLine.setInt(6, ol_supply_w_id);
                  stmtInsertOrderLine.setInt(7, ol_quantity);
                  stmtInsertOrderLine.setFloat(8, ol_amount);
                  stmtInsertOrderLine.setString(9, ol_dist_info);
                  stmtInsertOrderLine.addBatch();

            } // end-for

            //stmtInsertOrderLine.executeBatch();
            //stmtUpdateStock.executeBatch();
            //transCommit();
            //stmtInsertOrderLine.clearBatch();
            //stmtUpdateStock.clearBatch();

            total_amount *= (1+w_tax+d_tax)*(1-c_discount);

        } //// ugh :-), this is the end of the try block at the begining of this method /////////

        catch (SQLException ex) {
            System.out.println("\n--- Unexpected SQLException caught in NEW-ORDER Txn ---\n");
            while (ex != null) {
              System.out.println("Message:   " + ex.getMessage ());
              System.out.println("SQLState:  " + ex.getSQLState ());
              System.out.println("ErrorCode: " + ex.getErrorCode ());
              ex = ex.getNextException();
              System.out.println("");
            }
            complete = false;

        } catch (Exception e) {
            if (e instanceof IllegalAccessException) {
            	//e.printStackTrace();    
            }
            //e.printStackTrace();
            complete = false;

        } finally {
            try {
                terminalMessage("Performing ROLLBACK in NEW-ORDER Txn...");
                transRollback();
                stmtInsertOrderLine.clearBatch();
                stmtUpdateStock.clearBatch();
            } catch(Exception e1){
                
            }
        }
      	return complete;
    }
	
	public boolean TPCC_PaymentTransaction()
    {
    	
    	try
	    {
	    	if (conn.isClosed())
    		{
	       		Class.forName(driver);
				this.conn=DriverManager.getConnection(url,"sa","1234");
				conn.setAutoCommit(false);
	    	}
	    }
    	catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    	
    	gen = new Random(System.currentTimeMillis() * conn.hashCode());
    	
    	int w_id = randomNumber(1, configWhseCount, gen);
    	int d_id = randomNumber(1, 10, gen);

        int x = randomNumber(1, 100, gen);
        int c_d_id;
        int c_w_id;
        if(x <= 85)
        {
            c_d_id = d_id;
            c_w_id = w_id;
        }
        else
        {
            c_d_id = randomNumber(1, 10, gen);
            do
            {
                c_w_id = randomNumber(1, numWarehouses, gen);
            }
            while(c_w_id == w_id && numWarehouses > 1);
        }

        long y = randomNumber(1, 100, gen);
        boolean c_by_name;
        String c_last = null;
        int c_id = -1;
//                if(y <= 60)  {
            // 60% lookups by last name
//                    c_by_name = true;
//                    c_last = getLastName(gen);
//                    printMessage("Last name lookup = " + c_last);
//                } else {
//                    // 40% lookups by customer ID
        c_by_name = false;
        c_id = getCustomerID(gen);
//                }

        float h_amount = (float)(randomNumber(100, 500000, gen)/100.0);

        String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
        String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
        int namecnt;
        String c_first, c_middle, c_street_1, c_street_2, c_city, c_state, c_zip;
        String c_phone, c_credit = null, c_data = null, c_new_data, h_data;
        float c_credit_lim, c_discount, c_balance = 0;
        java.sql.Date c_since;

		/*
        Warehouse whse = new Warehouse();
        Customer  cust = new Customer();
        District  dist = new District();
        History   hist = new History();
		*/
        try
        {

              if (payUpdateWhse == null) {
                payUpdateWhse = conn.prepareStatement(
                  "UPDATE warehouse SET w_ytd = w_ytd + ?  WHERE w_id = ? ");
              }
              payUpdateWhse.setFloat(1,h_amount);
              payUpdateWhse.setInt(2,w_id);
              //result = payUpdateWhse.executeUpdate();
              //if(result == 0) throw new Exception("W_ID=" + w_id + " not found!");

              if (payGetWhse == null) {
                payGetWhse = conn.prepareStatement(
                  "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name" +
                  " FROM warehouse WHERE w_id = ?");
              }
              payGetWhse.setInt(1, w_id);
              //rs = payGetWhse.executeQuery();
              //if(!rs.next()) throw new Exception("W_ID=" + w_id + " not found!");
              w_street_1 = rs.getString("w_street_1");
              w_street_2 = rs.getString("w_street_2");
              w_city = rs.getString("w_city");
              w_state = rs.getString("w_state");
              w_zip = rs.getString("w_zip");
              w_name = rs.getString("w_name");
              //rs.close();
              //rs = null;

              if (payUpdateDist == null) {
                payUpdateDist = conn.prepareStatement(
                  "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?");
              }
              payUpdateDist.setFloat(1, h_amount);
              payUpdateDist.setInt(2, w_id);
              payUpdateDist.setInt(3, d_id);
              //result = payUpdateDist.executeUpdate();
              //if(result == 0) throw new Exception("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");

              if (payGetDist == null) {
                payGetDist = conn.prepareStatement(
                  "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name" +
                  " FROM district WHERE d_w_id = ? AND d_id = ?");
              }
              payGetDist.setInt(1, w_id);
              payGetDist.setInt(2, d_id);
              //rs = payGetDist.executeQuery();
              //if(!rs.next()) throw new Exception("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
              d_street_1 = rs.getString("d_street_1");
              d_street_2 = rs.getString("d_street_2");
              d_city = rs.getString("d_city");
              d_state = rs.getString("d_state");
              d_zip = rs.getString("d_zip");
              d_name = rs.getString("d_name");
              //rs.close();
              //rs = null;

            if(c_by_name) {
                // payment is by customer name
                  if (payCountCust == null) {
                    payCountCust = conn.prepareStatement(
                      "SELECT count(c_id) AS namecnt FROM customer " +
                      " WHERE c_last = ?  AND c_d_id = ? AND c_w_id = ?");
                  }
                  payCountCust.setString(1, c_last);
                  payCountCust.setInt(2, c_d_id);
                  payCountCust.setInt(3, c_w_id);
                  //rs = payCountCust.executeQuery();
                  //if(!rs.next()) throw new Exception("C_LAST=" + c_last + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
                  namecnt = rs.getInt("namecnt");
                  //rs.close();
                  rs = null;

                if (payCursorCustByName == null) {
                  payCursorCustByName = conn.prepareStatement(
                    "SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state, c_zip," +
                    "       c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since " +
                    "  FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? " +
                    "ORDER BY c_w_id, c_d_id, c_last, c_first ");
                }
                payCursorCustByName.setInt(1, c_w_id);
                payCursorCustByName.setInt(2, c_d_id);
                payCursorCustByName.setString(3, c_last);
                //rs = payCursorCustByName.executeQuery();
                //if(!rs.next()) throw new Exception("C_LAST=" + c_last + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
                if(namecnt%2 == 1) namecnt++;
                for(int i = 1; i < namecnt / 2; i++) rs.next();
                c_id = rs.getInt("c_id");
                c_first = rs.getString("c_first");
                c_middle = rs.getString("c_middle");
                c_street_1 = rs.getString("c_street_1");
                c_street_2 = rs.getString("c_street_2");
                c_city = rs.getString("c_city");
                c_state = rs.getString("c_state");
                c_zip = rs.getString("c_zip");
                c_phone = rs.getString("c_phone");
                c_credit = rs.getString("c_credit");
                c_credit_lim = rs.getFloat("c_credit_lim");
                c_discount = rs.getFloat("c_discount");
                c_balance = rs.getFloat("c_balance");
                c_since = rs.getDate("c_since");
                //rs.close();
                rs = null;
            } else {
              // payment is by customer ID
                if (payGetCust == null) {
                  payGetCust = conn.prepareStatement(
                    "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip," +
                    "       c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since " +
                    "  FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
                }
                payGetCust.setInt(1, c_w_id);
                payGetCust.setInt(2, c_d_id);
                payGetCust.setInt(3, c_id);
                //rs = payGetCust.executeQuery();
                //if(!rs.next()) throw new Exception("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
                c_last = rs.getString("c_last");
                c_first = rs.getString("c_first");
                c_middle = rs.getString("c_middle");
                c_street_1 = rs.getString("c_street_1");
                c_street_2 = rs.getString("c_street_2");
                c_city = rs.getString("c_city");
                c_state = rs.getString("c_state");
                c_zip = rs.getString("c_zip");
                c_phone = rs.getString("c_phone");
                c_credit = rs.getString("c_credit");
                c_credit_lim = rs.getFloat("c_credit_lim");
                c_discount = rs.getFloat("c_discount");
                c_balance = rs.getFloat("c_balance");
                c_since = rs.getDate("c_since");
                //rs.close();
                rs = null;
            }


            c_balance += h_amount;
            if(c_credit.equals("BC")) {  // bad credit

                if (payGetCustCdata == null) {
                  payGetCustCdata = conn.prepareStatement(
                    "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
                }
                payGetCustCdata.setInt(1, c_w_id);
                payGetCustCdata.setInt(2, c_d_id);
                payGetCustCdata.setInt(3, c_id);
                //rs = payGetCustCdata.executeQuery();
                //if(!rs.next()) throw new Exception("C_ID=" + c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");
                c_data = rs.getString("c_data");
                //rs.close();
                rs = null;

              c_new_data = c_id + " " + c_d_id + " " + c_w_id + " " + d_id + " " + w_id  + " " + h_amount + " |";
              if(c_data.length() > c_new_data.length()) {
                  c_new_data += c_data.substring(0, c_data.length()-c_new_data.length());
              } else {
                  c_new_data += c_data;
              }
              if(c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);

                if (payUpdateCustBalCdata == null) {
                  payUpdateCustBalCdata = conn.prepareStatement(
                    "UPDATE customer SET c_balance = ?, c_data = ? " +
                    " WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
                }
                payUpdateCustBalCdata.setFloat(1, c_balance);
                payUpdateCustBalCdata.setString(2, c_new_data);
                payUpdateCustBalCdata.setInt(3, c_w_id);
                payUpdateCustBalCdata.setInt(4, c_d_id);
                payUpdateCustBalCdata.setInt(5, c_id);
                //result = payUpdateCustBalCdata.executeUpdate();

              //if(result == 0) throw new Exception("Error in PYMNT Txn updating Customer C_ID=" + c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id);

            } else { // GoodCredit

                if (payUpdateCustBal == null) {
                  payUpdateCustBal = conn.prepareStatement(
                    "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
                }
                payUpdateCustBal.setFloat(1, c_balance);
                payUpdateCustBal.setInt(2, c_w_id);
                payUpdateCustBal.setInt(3, c_d_id);
                payUpdateCustBal.setInt(4, c_id);
                //result = payUpdateCustBal.executeUpdate();

              //if(result == 0) throw new Exception("C_ID=" + c_id + " C_W_ID=" + c_w_id + " C_D_ID=" + c_d_id + " not found!");

            }


            if(w_name.length() > 10) w_name = w_name.substring(0, 10);
            if(d_name.length() > 10) d_name = d_name.substring(0, 10);
            h_data = w_name + "    " + d_name;


            if (payInsertHist == null) {
              payInsertHist = conn.prepareStatement(
                "INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) " +
                " VALUES (?,?,?,?,?,?,?,?)");
            }
            payInsertHist.setInt(1, c_d_id);
            payInsertHist.setInt(2, c_w_id);
            payInsertHist.setInt(3, c_id);
            payInsertHist.setInt(4, d_id);
            payInsertHist.setInt(5, w_id);
            payInsertHist.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            payInsertHist.setFloat(7, h_amount);
            payInsertHist.setString(8, h_data);
            //payInsertHist.executeUpdate();

            transCommit();

            if (c_since != null)
            {
		    } else {
			}
            if(c_credit.equals("BC"))
            {
                if(c_data.length() > 50)
                {
                    int data_chunks = c_data.length() > 200 ? 4 : c_data.length()/50;
                //    for(int n = 1; n < data_chunks; n++)
                }
                else
                {
                }
            }
        }
        catch(Exception e)
        {
            //error("PAYMENT");
            //logException(e);
            //e.printStackTrace();
            try
            {
                terminalMessage("Performing ROLLBACK...");
                transRollback();
                return false;
            }
            catch(Exception e1)
            {
            	e1.printStackTrace();
           		//  error("PAYMENT-ROLLBACK");
              //  logException(e1);
            }
        }
        return true;
    } // end PaymentTransaction
    
    public boolean TPCC_StockLevelTransaction()
    {
    	try
	    {
	    	if (conn.isClosed())
    		{
	       		Class.forName(driver);
				this.conn=DriverManager.getConnection(url,"sa","1234");
				conn.setAutoCommit(false);
	    	}
	    }
    	catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    	
    	gen = new Random(System.currentTimeMillis() * conn.hashCode());
    	
    	int threshold = randomNumber(10, 20, gen);
    	int w_id = randomNumber(1, configWhseCount, gen);
    	int d_id = randomNumber(1, configDistPerWhse, gen);	
        int o_id = 0;
        int i_id = 0;
        int stock_count = 0;
		/*
        District  dist = new District();
        OrderLine orln = new OrderLine();
        Stock     stck = new Stock();
		*/
        printMessage("Stock Level Txn for W_ID=" + w_id + ", D_ID=" + d_id + ", threshold=" + threshold);

        try
        {
              if (stockGetDistOrderId == null) {
                stockGetDistOrderId = conn.prepareStatement(
                  "SELECT d_next_o_id" +
                  " FROM district" +
                  " WHERE d_w_id = ?" +
                  " AND d_id = ?");
              }
              stockGetDistOrderId.setInt(1, w_id);
              stockGetDistOrderId.setInt(2, d_id);
              //rs = stockGetDistOrderId.executeQuery();

              //if(!rs.next()) throw new Exception("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");
              o_id = rs.getInt("d_next_o_id");
              //rs.close();
              rs = null;
            printMessage("Next Order ID for District = " + o_id);

              if (stockGetCountStock == null) {
                stockGetCountStock = conn.prepareStatement(
                  "SELECT COUNT(DISTINCT (s_i_id)) AS stock_count" +
                  " FROM order_line, stock" +
                  " WHERE ol_w_id = ?" +
                  " AND ol_d_id = ?" +
                  " AND ol_o_id < ?" +
                  " AND ol_o_id >= ? - 20" +
                  " AND s_w_id = ?" +
                  " AND s_i_id = ol_i_id" +
                  " AND s_quantity < ?");
              }
              stockGetCountStock.setInt(1, w_id);
              stockGetCountStock.setInt(2, d_id);
              stockGetCountStock.setInt(3, o_id);
              stockGetCountStock.setInt(4, o_id);
              stockGetCountStock.setInt(5, w_id);
              stockGetCountStock.setInt(6, threshold);
              //rs = stockGetCountStock.executeQuery();

              //if(!rs.next()) throw new Exception("OL_W_ID=" + w_id + " OL_D_ID=" + d_id + " OL_O_ID=" + o_id + " (...) not found!");
              stock_count = rs.getInt("stock_count");
              //rs.close();
              rs = null;
        }
        catch(Exception e)
        {
        	return false;
        }
        return true;
    } // end StockLevelTransaction
    
    public boolean TPCC_OrderStatusTransaction()
    {
    	try
	    {
	    	if (conn.isClosed())
    		{
	       		Class.forName(driver);
				this.conn=DriverManager.getConnection(url,"sa","1234");
				conn.setAutoCommit(false);
	    	}
	    }
    	catch (Exception e)
    	{
    		e.printStackTrace();
    	}
    	
    	gen = new Random(System.currentTimeMillis() * conn.hashCode());
    	
    	int w_id = randomNumber(1, configWhseCount, gen);
    	int d_id = randomNumber(1, 10, gen);

        int y = randomNumber(1, 100, gen);
        String c_last = null;
        int c_id = -1;
        boolean c_by_name;
        if(y <= 60)
        {
            c_by_name = true;
            c_last = getLastName(gen);
        }
        else
        {
            c_by_name = false;
            c_id = getCustomerID(gen);
        }

        int namecnt, o_id = -1, o_carrier_id = -1;
        float c_balance;
        String c_first, c_middle;
        java.sql.Date entdate = null;
        Vector orderLines = new Vector();


        try
        {
            if(c_by_name)
            {
                if (ordStatCountCust == null) {
                  ordStatCountCust = conn.prepareStatement(
                    "SELECT count(*) AS namecnt FROM customer" +
                    " WHERE c_last = ?" +
                    " AND c_d_id = ?" +
                    " AND c_w_id = ?");
                }
                ordStatCountCust.setString(1, c_last);
                ordStatCountCust.setInt(2, d_id);
                ordStatCountCust.setInt(3, w_id);
                //rs = ordStatCountCust.executeQuery();

                //if(!rs.next()) throw new Exception("C_LAST=" + c_last + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");
                namecnt = rs.getInt("namecnt");
                //rs.close();
                rs = null;

                // pick the middle customer from the list of customers

                if (ordStatGetCust == null) {
                  ordStatGetCust = conn.prepareStatement(
                    "SELECT c_balance, c_first, c_middle, c_id FROM customer" +
                    " WHERE c_last = ?" +
                    " AND c_d_id = ?" +
                    " AND c_w_id = ?" +
                    " ORDER BY c_w_id, c_d_id, c_last, c_first");
                }
                ordStatGetCust.setString(1, c_last);
                ordStatGetCust.setInt(2, d_id);
                ordStatGetCust.setInt(3, w_id);
                //rs = ordStatGetCust.executeQuery();

                //if(!rs.next()) throw new Exception("C_LAST=" + c_last + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");
                if(namecnt%2 == 1) namecnt++;
                for(int i = 1; i < namecnt / 2; i++) rs.next();
                c_id = rs.getInt("c_id");
                c_first = rs.getString("c_first");
                c_middle = rs.getString("c_middle");
                c_balance = rs.getFloat("c_balance");
                //rs.close();
                rs = null;
            }
            else
            {
                if (ordStatGetCustBal == null) {
                  ordStatGetCustBal = conn.prepareStatement(
                    "SELECT c_balance, c_first, c_middle, c_last" +
                    " FROM customer" +
                    " WHERE c_id = ?" +
                    " AND c_d_id = ?" +
                    " AND c_w_id = ?");
                }
                ordStatGetCustBal.setInt(1, c_id);
                ordStatGetCustBal.setInt(2, d_id);
                ordStatGetCustBal.setInt(3, w_id);
                //rs = ordStatGetCustBal.executeQuery();

                //if(!rs.next()) throw new Exception("C_ID=" + c_id + " C_D_ID=" + d_id + " C_W_ID=" + w_id + " not found!");
                c_last = rs.getString("c_last");
                c_first = rs.getString("c_first");
                c_middle = rs.getString("c_middle");
                c_balance = rs.getFloat("c_balance");
                //rs.close();
                rs = null;
            }

            // find the newest order for the customer

            if (ordStatGetNewestOrd == null) {
              ordStatGetNewestOrd = conn.prepareStatement(
                "SELECT MAX(o_id) AS maxorderid FROM oorder" +
                " WHERE o_w_id = ?" +
                " AND o_d_id = ?" +
                " AND o_c_id = ?");
            }
            ordStatGetNewestOrd.setInt(1, w_id);
            ordStatGetNewestOrd.setInt(2, d_id);
            ordStatGetNewestOrd.setInt(3, c_id);
            //rs = ordStatGetNewestOrd.executeQuery();

            /*if(rs.next())
            {
              o_id = rs.getInt("maxorderid");
              rs.close();
              rs = null;

              // retrieve the carrier & order date for the most recent order.

              if (ordStatGetOrder == null) {
                ordStatGetOrder = conn.prepareStatement(
                  "SELECT o_carrier_id, o_entry_d" +
                  " FROM oorder" +
                  " WHERE o_w_id = ?" +
                  " AND o_d_id = ?" +
                  " AND o_c_id = ?" +
                  " AND o_id = ?");
              }
              ordStatGetOrder.setInt(1, w_id);
              ordStatGetOrder.setInt(2, d_id);
              ordStatGetOrder.setInt(3, c_id);
              ordStatGetOrder.setInt(4, o_id);
              rs = ordStatGetOrder.executeQuery();

              if(rs.next())
              {
                  o_carrier_id = rs.getInt("o_carrier_id");
                  entdate = rs.getDate("o_entry_d");
              }
            }
            rs.close();*/
            rs = null;

            // retrieve the order lines for the most recent order

            if (ordStatGetOrderLines == null) {
              ordStatGetOrderLines = conn.prepareStatement(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity," +
                " ol_amount, ol_delivery_d" +
                " FROM order_line" +
                " WHERE ol_o_id = ?" +
                " AND ol_d_id =?" +
                " AND ol_w_id = ?");
            }
            ordStatGetOrderLines.setInt(1, o_id);
            ordStatGetOrderLines.setInt(2, d_id);
            ordStatGetOrderLines.setInt(3, w_id);
            //rs = ordStatGetOrderLines.executeQuery();

            /*while(rs.next())
            {
                StringBuffer orderLine = new StringBuffer();
                orderLine.append("[");
                orderLine.append(rs.getLong("ol_supply_w_id"));
                orderLine.append(" - ");
                orderLine.append(rs.getLong("ol_i_id"));
                orderLine.append(" - ");
                orderLine.append(rs.getLong("ol_quantity"));
                orderLine.append(" - ");
                orderLine.append(formattedDouble(rs.getDouble("ol_amount")));
                orderLine.append(" - ");
                if(rs.getDate("ol_delivery_d") != null)
                    orderLine.append(rs.getDate("ol_delivery_d"));
                else
                    orderLine.append("99-99-9999");
                orderLine.append("]");
                orderLines.add(orderLine.toString());
            }
            rs.close();*/
            rs = null;
        }
        catch(Exception e)
        {
        	//e.printStackTrace();
        	return false;
        }
        return true;
    } // end OrderStatusTransaction
    
	public boolean TPCC_DeliveryTransaction(){
	
		try
	    {
	    	if (conn.isClosed())
    		{
	       		Class.forName(driver);
				this.conn=DriverManager.getConnection(url,"sa","1234");
				conn.setAutoCommit(false);
	    	}
	    }
    	catch (Exception e)
    	{
    		e.printStackTrace();
    	}		
    	
    		
		gen = new Random(System.currentTimeMillis() * conn.hashCode());
		
		int w_id = randomNumber(1, configWhseCount, gen);
		int o_carrier_id = randomNumber(1, 10, gen);
        int d_id, no_o_id, c_id;
        float ol_total;
        int[] orderIDs;
        int skippedDeliveries = 0;
        boolean newOrderRemoved;

		/*
        Oorder oorder = new Oorder();
        OrderLine order_line = new OrderLine();
        NewOrder new_order = new NewOrder();
        */
        int no_w_id = w_id;
        int no_d_id;

        try
        {
            orderIDs = new int[10];
            for(d_id = 1; d_id <= 10; d_id++)
            {
                no_d_id = d_id;

                do
                {
                    no_o_id = -1;

                    if (delivGetOrderId == null) {
                      delivGetOrderId = conn.prepareStatement(
                        "SELECT no_o_id FROM new_order WHERE no_d_id = ?" +
                        " AND no_w_id = ?" +
                        " ORDER BY no_o_id ASC");
                    }
                    delivGetOrderId.setInt(1, d_id);
                    delivGetOrderId.setInt(2, w_id);
                    //rs = delivGetOrderId.executeQuery();
                    //if(rs.next()) no_o_id = rs.getInt("no_o_id");
                    orderIDs[(int)d_id-1] = no_o_id;
                    //rs.close();
                    rs = null;

                    newOrderRemoved = false;
                    if(no_o_id != -1)
                    {
                      no_o_id = no_o_id;

                      if (delivDeleteNewOrder == null) {
                        delivDeleteNewOrder = conn.prepareStatement(
                          "DELETE FROM new_order" +
                          " WHERE no_d_id = ?" +
                          " AND no_w_id = ?" +
                          " AND no_o_id = ?");
                      }
                      delivDeleteNewOrder.setInt(1, d_id);
                      delivDeleteNewOrder.setInt(2, w_id);
                      delivDeleteNewOrder.setInt(3, no_o_id);
                      //result = delivDeleteNewOrder.executeUpdate();

                      //if(result > 0) newOrderRemoved = true;
                    }
                }
                while(no_o_id != -1 && !newOrderRemoved);


                if(no_o_id != -1)
                {
                      if (delivGetCustId == null) {
                        delivGetCustId = conn.prepareStatement(
                          "SELECT o_c_id" +
                          " FROM oorder" +
                          " WHERE o_id = ?" +
                          " AND o_d_id = ?" +
                          " AND o_w_id = ?");
                      }
                      delivGetCustId.setInt(1, no_o_id);
                      delivGetCustId.setInt(2, d_id);
                      delivGetCustId.setInt(3, w_id);
                      //rs = delivGetCustId.executeQuery();

                      //if(!rs.next()) throw new Exception("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");
                      c_id = rs.getInt("o_c_id");
                      //rs.close();
                      rs = null;

                      if (delivUpdateCarrierId == null) {
                        delivUpdateCarrierId = conn.prepareStatement(
                          "UPDATE oorder SET o_carrier_id = ?" +
                          " WHERE o_id = ?" +
                          " AND o_d_id = ?" +
                          " AND o_w_id = ?");
                      }
                      delivUpdateCarrierId.setInt(1, o_carrier_id);
                      delivUpdateCarrierId.setInt(2, no_o_id);
                      delivUpdateCarrierId.setInt(3, d_id);
                      delivUpdateCarrierId.setInt(4, w_id);
                      //result = delivUpdateCarrierId.executeUpdate();

                    //if(result != 1) throw new Exception("O_ID=" + no_o_id + " O_D_ID=" + d_id + " O_W_ID=" + w_id + " not found!");

                      if (delivUpdateDeliveryDate == null) {
                        delivUpdateDeliveryDate = conn.prepareStatement(
                          "UPDATE order_line SET ol_delivery_d = ?" +
                          " WHERE ol_o_id = ?" +
                          " AND ol_d_id = ?" +
                          " AND ol_w_id = ?");
                      }
                      delivUpdateDeliveryDate.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                      delivUpdateDeliveryDate.setInt(2,no_o_id);
                      delivUpdateDeliveryDate.setInt(3,d_id);
                      delivUpdateDeliveryDate.setInt(4,w_id);
                      //result = delivUpdateDeliveryDate.executeUpdate();

                      //if(result == 0) throw new Exception("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");

                      if (delivSumOrderAmount == null) {
                        delivSumOrderAmount = conn.prepareStatement(
                          "SELECT SUM(ol_amount) AS ol_total" +
                          " FROM order_line" +
                          " WHERE ol_o_id = ?" +
                          " AND ol_d_id = ?" +
                          " AND ol_w_id = ?");
                      }
                      delivSumOrderAmount.setInt(1, no_o_id);
                      delivSumOrderAmount.setInt(2, d_id);
                      delivSumOrderAmount.setInt(3, w_id);
                      //rs = delivSumOrderAmount.executeQuery();

                      //if(!rs.next()) throw new Exception("OL_O_ID=" + no_o_id + " OL_D_ID=" + d_id + " OL_W_ID=" + w_id + " not found!");
                      ol_total = rs.getFloat("ol_total");
                      //rs.close();
                      rs = null;

                    if (delivUpdateCustBalDelivCnt == null) {
                      delivUpdateCustBalDelivCnt = conn.prepareStatement(
                        "UPDATE customer SET c_balance = c_balance + ?" +
                        ", c_delivery_cnt = c_delivery_cnt + 1" +
                        " WHERE c_id = ?" +
                        " AND c_d_id = ?" +
                        " AND c_w_id = ?");
                    }
                    delivUpdateCustBalDelivCnt.setFloat(1, ol_total);
                    delivUpdateCustBalDelivCnt.setInt(2, c_id);
                    delivUpdateCustBalDelivCnt.setInt(3, d_id);
                    delivUpdateCustBalDelivCnt.setInt(4, w_id);
                    //result = delivUpdateCustBalDelivCnt.executeUpdate();

                    //if(result == 0) throw new Exception("C_ID=" + c_id + " C_W_ID=" + w_id + " C_D_ID=" + d_id + " not found!");
                }
            }

            conn.commit();

        }
        catch(Exception e)
        {
        	//e.printStackTrace();
            try
            {
                terminalMessage("Performing ROLLBACK...");
                conn.rollback();
                return false;
            }
            catch(Exception e1)
            {
            	e1.printStackTrace();
            }
        }

        return true;
    }
}
	
	/* ---------------------------------------------------------------------------------- */
/*		
	public class jdbcIO {

    public void insertOrder(PreparedStatement ordrPrepStmt, Oorder oorder) {
    
        try {

          ordrPrepStmt.setInt(1, o_id);
          ordrPrepStmt.setInt(2, o_w_id);
          ordrPrepStmt.setInt(3, o_d_id);
          ordrPrepStmt.setInt(4, o_c_id);
          ordrPrepStmt.setInt(5, o_carrier_id);
          ordrPrepStmt.setInt(6, o_ol_cnt); 
          ordrPrepStmt.setInt(7, o_all_local);
          Timestamp entry_d = new java.sql.Timestamp(o_entry_d);
          ordrPrepStmt.setTimestamp(8, entry_d);

          ordrPrepStmt.addBatch();
          
      } catch(SQLException se) { 
        System.out.println(se.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
       }

    }  // end insertOrder()

    public void insertNewOrder(PreparedStatement nworPrepStmt, NewOrder new_order) {
    
        try {
          nworPrepStmt.setInt(1, no_w_id); 
          nworPrepStmt.setInt(2, no_d_id); 
          nworPrepStmt.setInt(3, no_o_id); 

          nworPrepStmt.addBatch();              
          
      } catch(SQLException se) { 
        System.out.println(se.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
       }

    }  // end insertNewOrder()

    public void insertOrderLine(PreparedStatement orlnPrepStmt, OrderLine order_line) {
    
      try {
        orlnPrepStmt.setInt(1, ol_w_id);
        orlnPrepStmt.setInt(2, ol_d_id);
        orlnPrepStmt.setInt(3, ol_o_id);
        orlnPrepStmt.setInt(4, ol_number);
        orlnPrepStmt.setLong(5, ol_i_id);

        Timestamp delivery_d = new Timestamp(ol_delivery_d);
        orlnPrepStmt.setTimestamp(6, delivery_d);

        orlnPrepStmt.setDouble(7, ol_amount);
        orlnPrepStmt.setLong(8, ol_supply_w_id);
        orlnPrepStmt.setDouble(9, ol_quantity);
        orlnPrepStmt.setString(10, ol_dist_info); 

        orlnPrepStmt.addBatch();
    
      } catch(SQLException se) { 
        System.out.println(se.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
       }

    }  // end insertOrderLine()

}  // end class jdbcIO()
}

	class Warehouse implements Serializable {
	
		public int    w_id;  // PRIMARY KEY
		public float  w_ytd;
	  	public float  w_tax;
	  	public String w_name;
		public String w_street_1;
		public String w_street_2;
		public String w_city;
		public String w_state;
		public String w_zip;
		
		public String toString(){
			
	    	return (
		    	"\n***************** Warehouse ********************" +
		    	"\n*       w_id = " + w_id +
		    	"\n*      w_ytd = " + w_ytd +
		    	"\n*      w_tax = " + w_tax +
		      	"\n*     w_name = " + w_name +
		      	"\n* w_street_1 = " + w_street_1 +
		      	"\n* w_street_2 = " + w_street_2 +
		      	"\n*     w_city = " + w_city +
		      	"\n*    w_state = " + w_state +
		      	"\n*      w_zip = " + w_zip +
		      	"\n**********************************************"
		    );
	    }
	}  // end Warehouse
		    
	public class Stock implements Serializable {
  
      	public int    s_i_id;  //PRIMARY KEY 2
      	public int    s_w_id;  //PRIMARY KEY 1
      	public int    s_order_cnt;
      	public int    s_remote_cnt;
      	public int    s_quantity;
      	public float  s_ytd;
      	public String s_data;
      	public String s_dist_01;
      	public String s_dist_02;
      	public String s_dist_03;
      	public String s_dist_04;
      	public String s_dist_05;
      	public String s_dist_06;
      	public String s_dist_07;
      	public String s_dist_08;
      	public String s_dist_09;
     	public String s_dist_10;

      	public String toString()
      	{
            return (

              "\n***************** Stock ********************" +
              "\n*       s_i_id = " + s_i_id +
              "\n*       s_w_id = " + s_w_id +
              "\n*   s_quantity = " + s_quantity +
              "\n*        s_ytd = " + s_ytd +
              "\n*  s_order_cnt = " + s_order_cnt +
              "\n* s_remote_cnt = " + s_remote_cnt +
              "\n*       s_data = " + s_data +
              "\n*    s_dist_01 = " + s_dist_01 +
              "\n*    s_dist_02 = " + s_dist_02 +
              "\n*    s_dist_03 = " + s_dist_03 +
              "\n*    s_dist_04 = " + s_dist_04 +
              "\n*    s_dist_05 = " + s_dist_05 +
              "\n*    s_dist_06 = " + s_dist_06 +
              "\n*    s_dist_07 = " + s_dist_07 +
              "\n*    s_dist_08 = " + s_dist_08 +
              "\n*    s_dist_09 = " + s_dist_09 +
              "\n*    s_dist_10 = " + s_dist_10 +
              "\n**********************************************"
              );
      	}
	}  // end Stock
	
	public class OrderLine implements Serializable {
  
		  public int    ol_w_id;
		  public int    ol_d_id;
		  public int    ol_o_id;
		  public int    ol_number;
		  public int    ol_i_id;
		  public int    ol_supply_w_id;
		  public int    ol_quantity;
		  public long	  ol_delivery_d;
		  public float	ol_amount;
		  public String ol_dist_info;
		
		  public String toString()
		  {
		    return (
		      "\n***************** OrderLine ********************" +
		      "\n*        ol_w_id = " + ol_w_id +
		      "\n*        ol_d_id = " + ol_d_id +
		      "\n*        ol_o_id = " + ol_o_id +
		      "\n*      ol_number = " + ol_number +
		      "\n*        ol_i_id = " + ol_i_id +
		      "\n*  ol_delivery_d = " + ol_delivery_d +
		      "\n*      ol_amount = " + ol_amount +
		      "\n* ol_supply_w_id = " + ol_supply_w_id +
		      "\n*    ol_quantity = " + ol_quantity +
		      "\n*   ol_dist_info = " + ol_dist_info +
		      "\n**********************************************"
		      );
		  }

	}  // end OrderLine
	
	public class Oorder implements Serializable {
  
		  public int  o_id;
		  public int  o_w_id;
		  public int  o_d_id;
		  public int  o_c_id;
		  public int  o_carrier_id;
		  public int	o_ol_cnt;
		  public int 	o_all_local;
		  public long	o_entry_d;
		
		  public String toString()
		  {
		    java.sql.Timestamp entry_d = new java.sql.Timestamp(o_entry_d);
		
		    return (
		      "\n***************** Oorder ********************" +
		      "\n*         o_id = " + o_id +
		      "\n*       o_w_id = " + o_w_id +
		      "\n*       o_d_id = " + o_d_id +
		      "\n*       o_c_id = " + o_c_id +
		      "\n* o_carrier_id = " + o_carrier_id +
		      "\n*     o_ol_cnt = " + o_ol_cnt +
		      "\n*  o_all_local = " + o_all_local +
		      "\n*    o_entry_d = " + entry_d +
		      "\n**********************************************"
		      );
		  }
	}  // end Oorder
	
	public class NewOrder implements Serializable {
  
		  public int no_w_id;
		  public int no_d_id;
		  public int no_o_id;
		
		  public String toString()
		  {
		    return (
		      "\n***************** NewOrder ********************" +
		      "\n*      no_w_id = " + no_w_id +
		      "\n*      no_d_id = " + no_d_id +
		      "\n*      no_o_id = " + no_o_id +
		      "\n**********************************************"
		      );
		  }
	
	}  // end NewOrder
	
	public class Item implements Serializable {

		  public int   i_id; // PRIMARY KEY
		  public int   i_im_id;
		  public float i_price; 
		  public String i_name; 
		  public String i_data; 
		
		  public String toString()
		  {
		    return (
		      "\n***************** Item ********************" +
		      "\n*    i_id = " + i_id +
		      "\n*  i_name = " + i_name +
		      "\n* i_price = " + i_price +
		      "\n*  i_data = " + i_data +
		      "\n* i_im_id = " + i_im_id +
		      "\n**********************************************"
		      );
		  }
	}  // end Item
	
	public class History implements Serializable {
  
		  public int    h_c_id;
		  public int    h_c_d_id;
		  public int    h_c_w_id;
		  public int    h_d_id;
		  public int    h_w_id;
		  public long	  h_date;
		  public float	h_amount;
		  public String h_data;
		
		  public String toString()
		  {
		    return (
		      "\n***************** History ********************" +
		      "\n*   h_c_id = " + h_c_id +
		      "\n* h_c_d_id = " + h_c_d_id +
		      "\n* h_c_w_id = " + h_c_w_id +
		      "\n*   h_d_id = " + h_d_id +
		      "\n*   h_w_id = " + h_w_id +
		      "\n*   h_date = " + h_date +
		      "\n* h_amount = " + h_amount +
		      "\n*   h_data = " + h_data +
		      "\n**********************************************"
		      );
		  }
	
	}  // end History
	
	public class District implements Serializable {
 
		  public int    d_id;
		  public int    d_w_id;
		  public int    d_next_o_id;
		  public float	d_ytd;
		  public float	d_tax;
		  public String d_name;
		  public String d_street_1;
		  public String d_street_2;
		  public String d_city;
		  public String d_state;
		  public String d_zip;
		
		  public String toString()
		  {
		    return (
		      "\n***************** District ********************" +
		      "\n*        d_id = " + d_id +
		      "\n*      d_w_id = " + d_w_id +
		      "\n*       d_ytd = " + d_ytd +
		      "\n*       d_tax = " + d_tax +
		      "\n* d_next_o_id = " + d_next_o_id +
		      "\n*      d_name = " + d_name +
		      "\n*  d_street_1 = " + d_street_1 +
		      "\n*  d_street_2 = " + d_street_2 +
		      "\n*      d_city = " + d_city +
		      "\n*     d_state = " + d_state +
		      "\n*       d_zip = " + d_zip +
		      "\n**********************************************"
		      );
		  }

	}  // end District
	
	public class Customer implements Serializable {
 
	  public int    c_id;
	  public int    c_d_id;
	  public int    c_w_id;
	  public int    c_payment_cnt;
	  public int    c_delivery_cnt;
	  public long	  c_since;
	  public float	c_discount;
	  public float	c_credit_lim;
	  public float	c_balance;
	  public float	c_ytd_payment;
	  public String c_credit;
	  public String c_last;
	  public String c_first;
	  public String c_street_1;
	  public String c_street_2;
	  public String c_city;
	  public String c_state;
	  public String c_zip;
	  public String c_phone;
	  public String c_middle;
	  public String c_data;
	
	  public String toString()
	  {
	      java.sql.Timestamp since = new java.sql.Timestamp(c_since);
	
		    return (
		      "\n***************** Customer ********************" +
		      "\n*           c_id = " + c_id +
		      "\n*         c_d_id = " + c_d_id +
		      "\n*         c_w_id = " + c_w_id +
		      "\n*     c_discount = " + c_discount +
		      "\n*       c_credit = " + c_credit +
		      "\n*         c_last = " + c_last +
		      "\n*        c_first = " + c_first +
		      "\n*   c_credit_lim = " + c_credit_lim +
		      "\n*      c_balance = " + c_balance +
		      "\n*  c_ytd_payment = " + c_ytd_payment +
		      "\n*  c_payment_cnt = " + c_payment_cnt +
		      "\n* c_delivery_cnt = " + c_delivery_cnt +
		      "\n*     c_street_1 = " + c_street_1 +
		      "\n*     c_street_2 = " + c_street_2 +
		      "\n*         c_city = " + c_city +
		      "\n*        c_state = " + c_state +
		      "\n*          c_zip = " + c_zip +
		      "\n*        c_phone = " + c_phone +
		      "\n*        c_since = " + since +
		      "\n*       c_middle = " + c_middle +
		      "\n*         c_data = " + c_data +
		      "\n**********************************************"
		      );
		  }
	
	}  // end Customer
}*/


