import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class AddonService {

	static Logger logger;
	
	static String sql;
	static String path;
	static SimpleDateFormat sdf = new SimpleDateFormat("HHmm");
	static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
	static long waitingTime = 60*1000;
	
	public static void main(String[] args){

		loadProperties();
		
		if(logger==null){
			System.out.println("Setting logger fail");
			ErrorHandle("Setting logger fail");
			return;
		}
			
		//設定目的路徑
		//path=System.getProperty("user.dir"); 
		path="/CDR/script/tool/GPRS_flatrate/AddonServerProgramTest";
		String tempFileDir="";
		
		if(tempFileDir!=null&&"".equals(tempFileDir))
			path+="/"+tempFileDir;
		
		logger.info("File Path:"+path);
		
		boolean exit=false;
		

		while(!exit){
			String s = sdf.format(new Date());
			logger.info("Program check point..."+new Date());
			if("0010".equals(s)){
				logger.info("Proccess start:");
				
				//確認目標資料夾內容
				try {
					checkFile();
				} catch (Exception e) {
					ErrorHandle("",e);
					return;
				}
				//撈取資料
				selectData();
				logger.info("Proccess end.");
				sendMail("AddonServer Program Finished at "+new Date());
			}
			waiting(waitingTime);
		}
	}
	
	public static void waiting(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
		}
	}

	public static void checkFile() throws Exception{
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR)-1);
		String nFolder=sdf2.format(c.getTime());
		String nFolderPath=path+"/"+nFolder;
		
		File tempDir = new File(path);
		//確認路徑的位置類型
		if(tempDir.isDirectory()){
			logger.info("is folder!");
			//是否含有AddServer的資料
			File [] fl = tempDir.listFiles();

			logger.info("Check move file...");
			for(int i = 0 ; i<fl.length ; i++){
				File f = fl[i];
				if(f.getName().indexOf("AddServer")!=-1){
					//進行搬移動作
					newFolder(nFolderPath);
				
					if(moveFile(path,nFolderPath,f.getName())){
						//logger.info("move "+f.getName()+" to "+nFolder);
					}else{
						throw new Exception("move "+f.getName()+" from "+path+" to "+nFolderPath+" fail!");
					}
				}
			}
			logger.info("Check move file end...");
		}else if(tempDir.isFile()){
			logger.info("is file!");
			throw new Exception("Dir is Not correct!");
		}else{
			logger.info("unknowm!");
			throw new Exception("Dir is Not correct!");
		}
	}
	
	public static boolean moveFile(String sourceDir,String DestDir,String fileName){
		File f = new File(sourceDir+"/"+fileName);
		File f2 = new File(DestDir+"/"+fileName);
		return f.renameTo(f2);
	}
	
	public static void selectData(){
		logger.info("Select Data...");

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			
			conn = connectDB();
			
			if(conn==null)
				throw new Exception("Connection null");
			
			st = conn.createStatement();
			//select A
			sql = "SELECT A.S2TIMSI,A.SERVICECODE,A.STATUS,CASE  WHEN A.STARTDATE< TRUNC(SYSDATE)-60 THEN to_char(TRUNC(SYSDATE)-60,'yyyyMMdd') ELSE to_char(A.STARTDATE,'yyyyMMdd') END STARTDATE , to_char(TRUNC(SYSDATE)-1,'yyyyMMdd') ENDDATE,to_char(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "FROM ADDONSERVICE_N A "
					+ "WHERE (A.ENDDATE IS NULL OR A.ENDDATE> TRUNC(SYSDATE)-60) AND STATUS ='A'";

			rs = st.executeQuery(sql);
			logger.info("select status A :"+sql);
			
			while(rs.next()){
				createFile(rs.getString("S2TIMSI"),rs.getString("SERVICECODE"),rs.getString("STATUS"),rs.getString("STARTDATE"),rs.getString("ENDDATE"),rs.getString("APPLYDATE"));
			}
			
			rs=null;

			//select D
			sql = "SELECT A.S2TIMSI,A.SERVICECODE,A.STATUS, to_char(TRUNC(SYSDATE)-60,'yyyyMMdd') STARTDATE , to_char(A.ENDDATE,'yyyyMMdd') ENDDATE,to_char(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "FROM ADDONSERVICE_N A "
					+ "WHERE (A.ENDDATE IS NULL OR A.ENDDATE>= TRUNC(SYSDATE)-60) AND STATUS ='D'";
			
			rs = st.executeQuery(sql);
			logger.info("select status D :"+sql);
			
			while(rs.next()){
				createFile(rs.getString("S2TIMSI"),rs.getString("SERVICECODE"),rs.getString("STATUS"),rs.getString("STARTDATE"),rs.getString("ENDDATE"),rs.getString("APPLYDATE"));
			}
		} catch (Exception e) {
			ErrorHandle("Select error:",e);
		}finally{
			try {
				if(conn!=null)
					conn.close();
				if(st!=null)
					st.close();
				if(rs!=null)
					rs.close();
			} catch (SQLException e) {
			}
		}
	}
	
	public static void createFile(String IMSI,String SERVICECODE,String STATUS,String STARTDATE,String ENDDATE,String APPLYDATE){

		String fileName = "AddServer."+SERVICECODE.substring(2)+"."+IMSI+"."+APPLYDATE+".txt";
		String fileCont = IMSI+" "+SERVICECODE.substring(2)+" "+STARTDATE+" "+ENDDATE;
				//[IMSI][空格][SX代碼][空格][起始時間][空格][結束時間]

		File f = new File(path+"/"+fileName);
		BufferedWriter fw = null;
		try {
			f.createNewFile();
			fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, true), "UTF-8"));
			fw.write(fileCont);
		} catch (UnsupportedEncodingException e) {
			ErrorHandle("Create File error",e);
		} catch (FileNotFoundException e) {
			ErrorHandle("Create File error",e);
		} catch (IOException e) {
			ErrorHandle("Create File error",e);
		}finally{	
			try {
				if(fw!=null)
					fw.close();
			} catch (IOException e) {
			}
		}
	}
	public static void newFolder(String folderPath) {
		
		try {
			File myFilePath = new File(folderPath);
			if (!myFilePath.exists()) {
				myFilePath.mkdir();
				logger.info("create new folder :"+folderPath);
			}
		}catch(Exception e) {
			ErrorHandle("Create new folder error",e);
		}
	}

	/**
	 * 連線至DB1
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private static Connection connectDB() throws ClassNotFoundException, SQLException{
		//conn=tool.connDB(logger, DriverClass, URL, UserName, PassWord);
		Connection conn = null;

		String url="jdbc:oracle:thin:@10.42.1.80:1521:s2tbs";
		conn=connDB("oracle.jdbc.driver.OracleDriver", url,"s2tbsadm","s2tbsadm");
		logger.info("Connect to "+url);
	
		return conn;
	}
	public static Connection connDB(String DriverClass, String URL,
			String UserName, String PassWord) throws ClassNotFoundException, SQLException {
		Connection conn = null;

			Class.forName(DriverClass);
			conn = DriverManager.getConnection(URL, UserName, PassWord);
		return conn;
	}
	
	private static  void loadProperties(){
		Properties props =getProperties();
		PropertyConfigurator.configure(props);
		logger =Logger.getLogger(AddonService.class);
		logger.info("Logger Load Success!");
	}
	
	private static Properties getProperties(){
		Properties result=new Properties();
		
		result.setProperty("log4j.rootCategory", "DEBUG, stdout, FileOutput");
		
		result.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
		result.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
		result.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d [%5p] (%F:%L) - %m%n");
		
		result.setProperty("log4j.appender.FileOutput", "org.apache.log4j.DailyRollingFileAppender");
		result.setProperty("log4j.appender.FileOutput.layout", "org.apache.log4j.PatternLayout");
		result.setProperty("log4j.appender.FileOutput.layout.ConversionPattern", "%d [%5p] (%F:%L) - %m%n");
		result.setProperty("log4j.appender.FileOutput.DatePattern", "'.'yyyyMMdd");
		result.setProperty("log4j.appender.FileOutput.File", "AddonService.log");

		return result;
	}

	private static void ErrorHandle(String str, Exception e) {
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		String s = sw.toString();
		
		ErrorHandle(str+"\n"+s );
	}
	private static void ErrorHandle(String str) {
		if(logger!=null)
			logger.error(str);
		sendMail(str);
	}
	
	private static void sendMail(String msg){
		String ip ="";
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			StringWriter s = new StringWriter();
			e.printStackTrace(new PrintWriter(s));
			logger.error(e);
		}
		
		String mailReceiver = "Douglas.Chuang@sim2travel.com,ranger.kao@sim2travel.com";
		
		msg=msg+" from location "+ip;			
		
		String [] cmd=new String[3];
		cmd[0]="/bin/bash";
		cmd[1]="-c";
		cmd[2]= "/bin/echo \""+msg+"\" | /bin/mailx -s \"AddonService alert\" -r  ADDON_SERVICE_ALERT_MAIL "+mailReceiver+"." ; ;

		try{
			Process p = Runtime.getRuntime().exec (cmd);
			p.waitFor();
			System.out.println("send mail cmd:"+cmd);
		}catch (Exception e){
			System.out.println("send mail fail:"+msg);
		}
	}
}
