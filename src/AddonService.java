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

	static boolean testMode = false;
	
	
	static Logger logger;
	
	static String mails="";
	static String sql;
	static String path;
	static String tempFileDir;
	static SimpleDateFormat sdf = new SimpleDateFormat("HHmm");
	static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
	static long waitingTime = 60*1000;
	static int cMove = 0;
	static int cA = 0;
	static int cD = 0;
	static int cU = 0;
	static int cCreate = 0;
	static int cCheck = 0;
	static int preCount = 0;
	static int toleration = 15; //%
	static int beforeDay = 60;
	static int realNumber = 0;
	
	public static void main(String[] args){

		loadProperties();
		
		if(logger==null){
			System.out.println("Setting logger fail");
			ErrorHandle("Setting logger fail");
			return;
		}
			
		//設定目的路徑
		//path=System.getProperty("user.dir"); 
		//path="/CDR/script/tool/GPRS_flatrate/AddonServerProgramTest";
		if(testMode){
			path="C:/Users/ranger.kao/Desktop/1104Addon";
		}else{
			path="/CDR/script/tool/GPRS_flatrate/inputfile";
		}
		tempFileDir="/temp";

		logger.info("File Path:"+path);
		
		boolean exit=false;
		
		//20151105調整by參數數量決定執行時間，無參數則為立即性一次啟動
		if(args.length==0){
				proccess();
		}else{
			String runTime = args[0];
			
			logger.info("Set run time at "+runTime+"(hh24mi)");
			
			while(!exit){
				String s = sdf.format(new Date());
				logger.info("Program check point..."+new Date());
				if(runTime.equals(s)){
					proccess();
				}
				waiting(waitingTime);
			}
		}
		
		
	}
	
	public static void waiting(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
		}
	}
	
	public static void proccess(){
		mails = "";
		logger.info("Proccess start:");
		try {
			//確認目標資料夾內容
			checkFile();
			//新建暫存資料夾
			newFolder(path+tempFileDir);
			//撈取資料
			selectData();
			//確認資料數量
			reCheckFile();
			
			
		} catch (Exception e) {
			ErrorHandle("",e);
			return;
		}finally{
			//刪除暫存資料夾
			delFilesinFolder(path+tempFileDir,true);
		}
		logger.info("Proccess end.");
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR)-beforeDay+1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		
		mails = "AddonServer Program Finished at "+new Date()+".\n"
				+ "Date from "+c.getTime()+"~"+new Date()+".\n"+mails;
		sendMail(mails);
	}

	public static void checkFile() throws Exception{
		cMove = 0;
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR)-1);
		String nFolder=sdf2.format(c.getTime());
		String nFolderPath=path+"/"+nFolder;
		
		File tempDir = new File(path);
		//確認路徑的位置類型
		if(tempDir.isDirectory()){
			logger.info("is folder!");

			//避免目的地有重複檔案造成錯誤
			//delFilesinFolder(nFolderPath);
			
			File nf = new File(nFolderPath);
			//避免目的地有重複檔案造成錯誤
			if(nf.exists()){
				File [] fl = nf.listFiles();
				String nfd = path+"/"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
				newFolder(nfd);
				for(int i = 0 ; i<fl.length ; i++){
					File f = fl[i];
					if(f.getName().indexOf("AddServer")!=-1){
						//進行搬移動作				
						if(moveFile(nFolderPath,nfd,f.getName())){
							//logger.info("move "+f.getName()+" to "+nFolder);
							cMove += 1;
						}else{
							throw new Exception("move "+f.getName()+" from "+path+" to "+nFolderPath+" fail!");
						}
					}
				}
			}
				
			newFolder(nFolderPath);
			
			logger.info("Check move file...");
			//是否含有AddServer的資料
			File [] fl = tempDir.listFiles();
			for(int i = 0 ; i<fl.length ; i++){
				File f = fl[i];
				if(f.getName().indexOf("AddServer")!=-1){
					//進行搬移動作				
					if(moveFile(path,nFolderPath,f.getName())){
						//logger.info("move "+f.getName()+" to "+nFolder);
						cMove += 1;
					}else{
						throw new Exception("move "+f.getName()+" from "+path+" to "+nFolderPath+" fail!");
					}
				}
			}
			logger.info("Check move file end...(total moved "+cMove+" files)");
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
		cCreate = 0;
		cA = 0;
		cD = 0;
		cU = 0;
		realNumber = 0;
		logger.info("Select Data...");

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			
			conn = connectDB();
			
			if(conn==null)
				throw new Exception("Connection null");
			
			st = conn.createStatement();
			
			//20151230 改變成先D再A
			//select D
			sql = "SELECT A.S2TIMSI,A.SERVICECODE,A.STATUS,CASE  WHEN A.STARTDATE< TRUNC(SYSDATE)-"+beforeDay+" THEN to_char(TRUNC(SYSDATE)-"+beforeDay+",'yyyyMMdd') ELSE to_char(A.STARTDATE,'yyyyMMdd') END STARTDATE , CASE WHEN A.ENDDATE IS NULL THEN to_char(TRUNC(SYSDATE)-1,'yyyyMMdd') ELSE to_char(A.ENDDATE,'yyyyMMdd') END ENDDATE,to_char(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "FROM ADDONSERVICE_N A "
					+ "WHERE (A.ENDDATE IS NULL OR A.ENDDATE>= TRUNC(SYSDATE)-"+beforeDay+") AND STATUS ='D'";
			
			//20151215 測試指令
			/*sql = "SELECT A.S2TIMSI,A.SERVICECODE,A.STATUS, to_char(TRUNC(to_date('2015/11/04','yyyy/MM/dd'))-"+beforeDay+",'yyyyMMdd') STARTDATE , to_char(A.ENDDATE,'yyyyMMdd') ENDDATE,to_char(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "FROM ADDONSERVICE_N A "
					+ "WHERE (A.ENDDATE IS NULL OR A.ENDDATE> TRUNC(to_date('2015/11/04','yyyy/MM/dd'))-"+beforeDay+") AND STATUS ='D' AND A.STARTDATE<TO_DATE('2015/11/04','yyyy/MM/dd')";*/
			rs = st.executeQuery(sql);
			logger.info("select status D :"+sql);
			
			while(rs.next()){
				createFile(rs.getString("S2TIMSI"),rs.getString("SERVICECODE"),rs.getString("STARTDATE"),rs.getString("ENDDATE"),rs.getString("APPLYDATE"));
			}
			cD = cCreate;
			logger.info("Created D "+cD);
			
			rs.close();
			rs=null;
			
			
			//select A
			sql = "SELECT A.S2TIMSI,A.SERVICECODE,A.STATUS,CASE  WHEN A.STARTDATE< TRUNC(SYSDATE)-"+beforeDay+" THEN to_char(TRUNC(SYSDATE)-"+beforeDay+",'yyyyMMdd') ELSE to_char(A.STARTDATE,'yyyyMMdd') END STARTDATE , CASE WHEN A.ENDDATE IS NULL THEN to_char(TRUNC(SYSDATE)-1,'yyyyMMdd') ELSE to_char(A.ENDDATE,'yyyyMMdd') END ENDDATE,to_char(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "FROM ADDONSERVICE_N A "
					+ "WHERE (A.ENDDATE IS NULL OR A.ENDDATE> TRUNC(SYSDATE)-"+beforeDay+") AND STATUS ='A'";
			
			//20151215 測試指令
			/*sql = "SELECT A.S2TIMSI,A.SERVICECODE,A.STATUS,CASE  WHEN A.STARTDATE< TRUNC(to_date('2015/11/04','yyyy/MM/dd'))-"+beforeDay+" THEN to_char(TRUNC(to_date('2015/11/04','yyyy/MM/dd'))-"+beforeDay+",'yyyyMMdd') ELSE to_char(A.STARTDATE,'yyyyMMdd') END STARTDATE , to_char(TRUNC(to_date('2015/11/04','yyyy/MM/dd'))-1,'yyyyMMdd') ENDDATE,to_char(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "FROM ADDONSERVICE_N A "
					+ "WHERE (A.ENDDATE IS NULL OR A.ENDDATE> TRUNC(to_date('2015/11/04','yyyy/MM/dd'))-"+beforeDay+") AND STATUS ='A' AND A.STARTDATE<TO_DATE('2015/11/04','yyyy/MM/dd')";*/

			rs = st.executeQuery(sql);
			logger.info("select status A :"+sql);
			
			while(rs.next()){
				createFile(rs.getString("S2TIMSI"),rs.getString("SERVICECODE"),rs.getString("STARTDATE"),rs.getString("ENDDATE"),rs.getString("APPLYDATE"));
			}
			cA = cCreate - cD;
			logger.info("Created A "+cA);

			rs.close();
			rs=null;
			
			//20160824
			//select 美國流量包
			sql = "select A.IMSI S2TIMSI,'SX100' SERVICECODE,CASE  WHEN A.START_DATE< to_char(TRUNC(SYSDATE)-"+beforeDay+",'yyyyMMdd') THEN to_char(TRUNC(SYSDATE)-"+beforeDay+",'yyyyMMdd') ELSE A.START_DATE END STARTDATE , CASE WHEN A.END_DATE IS NULL THEN to_char(TRUNC(SYSDATE)-1,'yyyyMMdd') ELSE A.END_DATE END ENDDATE,A.START_DATE APPLYDATE "
					+ "from HUR_VOLUME_POCKET A "
					+ "where (A.END_DATE IS NULL OR A.END_DATE> to_char(TRUNC(SYSDATE)-"+beforeDay+",'yyyyMMdd')) AND A.TYPE=0 AND A.CANCEL_TIME IS NULL ";
			

			rs = st.executeQuery(sql);
			logger.info("select status 美國上網包 :"+sql);
			
			while(rs.next()){
				createFile(rs.getString("S2TIMSI"),rs.getString("SERVICECODE"),rs.getString("STARTDATE"),rs.getString("ENDDATE"),rs.getString("APPLYDATE"));
			}
			cU = cCreate - cD - cA;
			logger.info("Created 美國上網包 "+cA);
			
			logger.info("total ceated "+cCreate+" files.");
			
			rs.close();
			rs=null;
			
			//select realCreated number
			sql = "SELECT COUNT(1) CD "
					+ "FROM ( "
					+ "			SELECT DISTINCT A.S2TIMSI,A.SERVICECODE,TO_CHAR(A.STARTDATE,'yyyyMMdd') APPLYDATE "
					+ "			FROM ADDONSERVICE_N A "
					+ "			WHERE (A.ENDDATE IS NULL OR A.ENDDATE> TRUNC(SYSDATE)-"+beforeDay+"))";
			
			rs = st.executeQuery(sql);
			logger.info("select status A & D :"+sql);
			
			while(rs.next()){
				realNumber = rs.getInt("CD");
			}
			
			rs.close();
			rs=null;
			
			//select realCreated number
			sql = "select count(1) CD from HUR_VOLUME_POCKET A where A.CANCEL_TIME is null and A.TYPE = 0";
			
			rs = st.executeQuery(sql);
			logger.info("select US pocket:"+sql);
			
			while(rs.next()){
				realNumber += rs.getInt("CD");
			}
			
			
			logger.info("real created Number "+realNumber);
			
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

			mails +="Created file number of A/D is "+cA+"/"+cD+".\n";
			mails +="Created file number of US pocket is "+cU+".\n";
			mails +="Total ceated "+cCreate+" files.\n";
			mails +="real ceated "+realNumber+" files.\n";
		}
	}
	public static void reCheckFile() throws Exception{
		logger.info("reCheck File...");
		cCheck = 0;

		long cSize = 0;
		long fSize = 0;
		File tempDir = new File(path+tempFileDir);
		//確認路徑的位置類型
		if(tempDir.isDirectory()){
			logger.info("is folder!");
			//是否含有AddServer的資料
			File [] fl = tempDir.listFiles();
			fSize = fl[0].length();
			logger.info("Check move file...");
			for(int i = 0 ; i<fl.length ; i++){
				File f = fl[i];
				if(f.getName().indexOf("AddServer")!=-1){
					cCheck +=1;
					cSize+=f.length();
				}
			}
			logger.info("Have "+cCheck+" files in folder.");
			
			if(cCheck!=cCreate && cCheck!=realNumber)
				throw new Exception("Created file number not match existed file number!");
				
			//20160824 cancel
			//確認資料量大小是否正確
			if(cSize != fSize*(realNumber))
				throw new Exception("Created file size not match!("+cSize+" is not equal to "+fSize+"*"+realNumber+")");
			
			logger.info(""+cSize+" is equal to "+fSize+"*"+realNumber+".");
			mails += ""+cSize+" is equal to "+fSize+"*"+realNumber+".\n";
			
			
			//確認與前次數量差異
			double diff = Math.abs(preCount-realNumber);
			double differentiae = diff/realNumber*100;
			if(preCount!=0 && differentiae>toleration)
				throw new Exception("(("+preCount+"-"+realNumber+")/"+realNumber+")*100 > "+toleration+"(%). Innomal number differentiae.");
			
			logger.info("(("+preCount+"-"+realNumber+")/"+realNumber+")*100 < "+toleration+"(%) is in normal range.");
			mails += "(("+preCount+"-"+realNumber+")/"+realNumber+")*100 < "+toleration+"(%) is in normal range.\n";
			preCount = realNumber;
			
			//將檔案搬回正式資料夾
			for(int i = 0 ; i<fl.length ; i++){
				File f = fl[i];
				if(f.getName().indexOf("AddServer")!=-1){
					moveFile(path+tempFileDir, path,f.getName());
				}
			}
			logger.info("Move Files from "+path+tempFileDir+" to "+path+".");
				
		}else if(tempDir.isFile()){
			logger.info("is file!");
			throw new Exception("Dir is Not correct!");
		}else{
			logger.info("unknowm!");
			throw new Exception("Dir is Not correct!");
		}
	}
	public static void createFile(String IMSI,String SERVICECODE,String STARTDATE,String ENDDATE,String APPLYDATE){

		String fileName = "AddServer."+SERVICECODE.substring(2)+"."+IMSI+"."+APPLYDATE+".txt";
		String fileCont = IMSI+" "+SERVICECODE.substring(2)+" "+STARTDATE+" "+ENDDATE;
				//[IMSI][空格][SX代碼][空格][起始時間][空格][結束時間]

		FileOutputStream out = null;
		try {
			out = new FileOutputStream(path+tempFileDir+"/"+fileName);
			out.write(fileCont.getBytes("UTF-8"));
			cCreate += 1;
		} catch (FileNotFoundException e) {
			ErrorHandle("Create File error",e);
		} catch (UnsupportedEncodingException e) {
			ErrorHandle("Create File error",e);
		} catch (IOException e) {
			ErrorHandle("Create File error",e);
		}finally{	
			if(out!=null)
				try {
					out.close();
				} catch (IOException e) {
				}
		}
		
		
		
		/*PrintWriter writer = null;
		try {
			writer = new PrintWriter(path+tempFileDir+"/"+fileName, "UTF-8");
			writer.println(fileCont);		
			cCreate += 1;
		} catch (FileNotFoundException e) {
			ErrorHandle("Create File error",e);
		} catch (UnsupportedEncodingException e) {
			ErrorHandle("Create File error",e);
		} catch (IOException e) {
			ErrorHandle("Create File error",e);
		}finally{	
			if(writer!=null)
				writer.close();
		}*/
		
		
		/*File f = new File(path+tempFileDir+"/"+fileName);
		BufferedWriter fw = null;
		try {
			f.createNewFile();
			fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, true), "UTF-8"));
			fw.write(fileCont);
			cCreate += 1;
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
		}*/
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
	
	public static void delFilesinFolder(String folderPath) {
		delFilesinFolder(folderPath,false);
	}
	
	public static void delFilesinFolder(String folderPath,boolean delfolder) {
		int count = 0;
		try {
			File myFilePath = new File(folderPath);
			if (myFilePath.exists()) {
				logger.info("Delete File in "+folderPath);
				File f[] = myFilePath.listFiles();
				for(int i=0;i<f.length;i++ ){
					f[i].delete();
					count ++;
				}
				logger.info("Deleted "+count+" files.");
			}
			if(delfolder)
				myFilePath.delete();
			
		}catch(Exception e) {
			ErrorHandle("Delete nfolder error",e);
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

		String url = null;
		if(testMode){
			url="jdbc:oracle:thin:@10.42.1.101:1521:S2TBSDEV";
			conn=connDB("oracle.jdbc.driver.OracleDriver", url,"foyadev","foyadev");
		}else{
			url="jdbc:oracle:thin:@10.42.1.80:1521:s2tbs";
			conn=connDB("oracle.jdbc.driver.OracleDriver", url,"s2tbsadm","s2tbsadm");
		}		
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
		
		String mailReceiver = "Douglas.Chuang@sim2travel.com,yvonne.lin@sim2travel.com,ranger.kao@sim2travel.com";
		
		msg=msg+" from location "+ip;			
		
		String [] cmd=new String[3];
		cmd[0]="/bin/bash";
		cmd[1]="-c";
		cmd[2]= "/bin/echo \""+msg+"\" | /bin/mailx -s \"AddonService alert\" -r  ADDON_SERVICE_ALERT_MAIL "+mailReceiver+"." ; ;

		try{
			Process p = Runtime.getRuntime().exec (cmd);
			p.waitFor();
			if(logger!=null)
				logger.info("send mail cmd:"+cmd[0]+"\n"+cmd[1]+"\n"+cmd[2]);
		}catch (Exception e){
			if(logger!=null)
				logger.error("send mail fail:"+msg);
		}
	}
}
