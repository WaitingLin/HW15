import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PostgreSQLJDBC extends Thread {
	
	/*輸入:資料夾路徑   輸出:裡面所有的檔案名稱(String[])*/
	public static String[] getFileList(String folderPath) throws IOException{
        	//String folderPath = "C:\\";//資料夾路徑
			String[] list = null;
            try{
               java.io.File folder = new java.io.File(folderPath);
               list = folder.list();
             }catch(Exception e){
                      System.out.println("'"+folderPath+"'此資料夾不存在");
             }              
			 return list;
    }
	
	/*輸入:String[] 輸出:印出此String*/
	public static void printlist(String[] list){
		  for(int i = 0; i < list.length; i++){
              System.out.println(list[i]);
         }
	}
	
	/*輸入:src 輸出:第七行以後的List[]*/
	public static String[] getfile(String src) throws IOException{
		ArrayList<String> data = new ArrayList<String>();
		 BufferedReader br = new BufferedReader(new FileReader(src));
		 try {
		        String line = br.readLine();
		        line = br.readLine();line = br.readLine();line = br.readLine();
		        line = br.readLine();line = br.readLine();line = br.readLine(); ///讀六行 只取第七行後的資料
		        while (line != null) {
		        	data.add(line);
		            line = br.readLine();
		        }
		        
		   }finally {
		        br.close();
		   }
		 String[] data1 = new String[data.size()];
		 for(int j = 0; j < data.size(); j++)
		 {
			 data1[j] = data.get(j);
		 }
		 return data1;
	}
	
	/*在postgresql產生trace0~5的Table*/
	public static void createPosgresqlTable(){
		Connection c = null;
	    Statement stmt = null;
	    String sql;
	    try {
	    	Class.forName("org.postgresql.Driver");
	    	c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/try_postgres","postgres", "f74001234");
	    	//System.out.println("postgresql database 連結成功!");
	    	/*--Create Table--*/
	    	stmt = c.createStatement();
	    	for(int i=0;i<6;i++){
	    		sql = "CREATE TABLE trace"+i+
	  	    	      "(ID INT PRIMARY KEY      NOT NULL, " +
	  	    	      " Uid            CHAR(5), " +
	  	    	      " Date           CHAR(15), " +
	  	    	      " Time           CHAR(15), " +
	  	    	      " Lat	           CHAR(30), " +
	  	    	      " Lon            CHAR(30))";
	  	    	stmt.executeUpdate(sql);  
	    	}
	    } catch ( Exception e ) {
	        //System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	        System.out.println("postgresql create table fail !");
	        System.exit(0);
	    }
	}
	
	/*將postgresql的trace0~5的table drop*/
	public static void dropPosgresqlTable(){
		Connection c = null;
	    Statement stmt = null;
	    String sql;
	    try {
	    	Class.forName("org.postgresql.Driver");
	    	c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/try_postgres","postgres", "f74001234");
	    	//System.out.println("postgresql database 連結成功!");
	    	/*--Drop Table--*/
	    	stmt = c.createStatement();
	    	for(int i=0;i<6;i++){
	    		sql = "DROP TABLE trace"+i;
	  	    	stmt.executeUpdate(sql);  
	    	}
	    } catch ( Exception e ) {
	        //System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	        System.out.println("postgresql drop table fail !");
	        System.exit(0);
	    }
	}
	
	/*(Posgresql) Return Uid=003 Date=2008-10-23 Time=17:58:54 Lat=39.999844 Lon=116.326752 Query time*/
	public static long posgresqlQueryTime(){
		long startTime = System.currentTimeMillis();
		Connection c = null;
	    Statement stmt = null;
		try {
	    	   Class.forName("org.postgresql.Driver");
	    	   /*--連結database--*/
	    	   c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/try_postgres","postgres", "f74001234");		
	    	   c.setAutoCommit(false);
	    	   stmt = c.createStatement();
	    	   ResultSet rs = stmt.executeQuery( "SELECT * FROM trace3 WHERE Uid='003' AND Date='2008-10-23'"
	    	   		+ "AND Time='17:58:54' AND Lat='39.999844' AND Lon='116.326752';" );
	    	   /*while ( rs.next() ) {
	    		   String  Uid = rs.getString("Uid");
	    		   String  Time = rs.getString("Date");
	    		   String  Date = rs.getString("Time");
	    		   String  Lat = rs.getString("Lat");
	    		   String  Lon= rs.getString("Date");
	    		   System.out.println( "UID = " + Uid ); 
	    		   System.out.println( "Time = " + Date );
	    		   System.out.println( "Date = " + Time );
	    		   System.out.println( "Lat = " + Lat );
	    		   System.out.println( "Lon = " + Lon );
	    		   System.out.println();
	    	   }*/	    	   
		}catch ( Exception e ) {
	         System.err.println( e.getClass().getName()+": "+ e.getMessage() );
	         System.exit(0);
	    }
		long endTime = System.currentTimeMillis();
		return endTime-startTime;
	}
	
	/*(Mongodb) Return Uid=003 Date=2008-10-23 Time=17:58:54 Lat=39.999844 Lon=116.326752 Query time*/
	public static long mongodbQueryTime(){
		try {
			long startTime = System.currentTimeMillis();
			MongoClient mongoClient = new MongoClient(); 
	 	   	DB db = mongoClient.getDB( "try_mongodb" );
			DBCollection coll = db.getCollection("trace3");
			DBCursor cursor = coll.find();
	 	   	BasicDBObject query = new BasicDBObject("Lat", 39.999844); 	   
	 	   	cursor = coll.find(query);	 	  
	 	   	long endTime = System.currentTimeMillis();
	 		cursor.close();
	 		  return endTime-startTime;
		}catch ( Exception e ) {
		 	System.out.println("error");
		 	return 0;  
		}
	}
	/*Insert Data to posgresql trace0~5 table*/
	public static void insertPosgresqlData(int n, String[] list){
		System.out.println("thread "+n+" is running...");
		String []data = null;
		String[] parts = null;
		Statement stmt = null;
		String sql;
		int key=0;
    	try {
    		Class.forName("org.postgresql.Driver");
	    	Connection c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/try_postgres","postgres", "f74001234");
			for(int j=0;j<list.length;j++){
				data=getfile("data/00"+n+"/Trajectory/"+list[j]);
				for(int i = 0; i < data.length; i++){
					parts = data[i].split(",");
					stmt = c.createStatement();
		    	    sql = "INSERT INTO trace"+n+" (ID,Uid,Date,Time,Lat,Lon) "
		    	    + "VALUES ("+key+", '00"+n+"' , '"+ parts[5] +"' , '"+ parts[6] +"' , '"+ parts[0] +"' , '"+ parts[1] +"');";
		    	    //System.out.println(sql);
		    	    stmt.executeUpdate(sql);
		    	    key++;
		         }
			}
	    	
		} catch (Exception e) {
			e.printStackTrace();
		} 
		System.out.println("thread "+n+" is done!");
	}
	
	/*在Mongodb產生trace0~5的collection && Insert Data*/
	public static void insertMongodbData(int n, String[] list){
		 System.out.println("thread "+n+" is running...");
		 String []data = null;
		 String[] parts = null;
		 int key=0;
		 DB db=null;
		    try {
		    	MongoClient mongoClient = new MongoClient();
		    	db = mongoClient.getDB( "try_mongodb" );
		    	//System.out.println("Mongodb 連結成功!");
		    	DBCollection coll;
		    	for(int j=0;j<list.length;j++){
					data=getfile("data/00"+n+"/Trajectory/"+list[j]);
					for(int i = 0; i < data.length; i++){
						parts = data[i].split(",");
						coll = db.getCollection("trace"+n);
						BasicDBObject doc = new BasicDBObject("ID", key)
				           .append("Uid", "00"+n)
				           .append("Date", parts[5])
				           .append("Time", parts[6])
				           .append("Lat", parts[0])
				           .append("Lon", parts[1])
				           ;
				    	   coll.insert(doc);
			    	    key++;
			         }
				}		    			    	
		    }catch ( Exception e ) {
		    	   //System.err.println( e.getClass().getName()+": "+ e.getMessage() );
		    	   System.out.println("Mongodb insert fail!");
		           System.exit(0);
		    } 
		    System.out.println("thread "+n+" is done!");
	}
	
	
	/*--------------------------------Main------------------------------------------------*/
	public static void main(String args[]) throws IOException {
	    /*--Mongodb--*/ 
	   
		/*------Folder file list-------*/
		final String [] fileList000 = getFileList("data/000/Trajectory"); 
		final String [] fileList001 = getFileList("data/001/Trajectory");
		final String [] fileList002 = getFileList("data/002/Trajectory");
		final String [] fileList003 = getFileList("data/003/Trajectory");
		final String [] fileList004 = getFileList("data/004/Trajectory");
		final String [] fileList005 = getFileList("data/005/Trajectory");		
		
		/*------My thread work postgresql insert-----*/
		Runnable p0 = new Runnable() {
			public void run() {
				insertPosgresqlData(0,fileList000);
			}
		};
		Runnable p1 = new Runnable() {
			public void run() {
				insertPosgresqlData(1,fileList001);
			}
		};
		Runnable p2 = new Runnable() {
			public void run() {
				insertPosgresqlData(2,fileList002);
			}
		};
		Runnable p3 = new Runnable() {
			public void run() {
				insertPosgresqlData(3,fileList003);
			}
		};
		Runnable p4 = new Runnable() {
			public void run() {
				insertPosgresqlData(4,fileList004);
			}
		};
		Runnable p5 = new Runnable() {
			public void run() {
				insertPosgresqlData(5,fileList005);
			}
		};
		
		/*------My thread work mongodb insert-----*/
		Runnable m0 = new Runnable() {
			public void run() {
				insertMongodbData(0,fileList000);
			}
		};
		Runnable m1 = new Runnable() {
			public void run() {
				insertMongodbData(1,fileList001);
			}
		};
		Runnable m2 = new Runnable() {
			public void run() {
				insertMongodbData(2,fileList002);
			}
		};
		Runnable m3 = new Runnable() {
			public void run() {
				insertMongodbData(3,fileList003);
			}
		};
		Runnable m4 = new Runnable() {
			public void run() {
				insertMongodbData(4,fileList004);
			}
		};
		Runnable m5 = new Runnable() {
			public void run() {
				insertMongodbData(5,fileList005);
			}
		};
				   
	   /*---------------------Postgresql-------------------------*/
	   /*--Create thread--*/
	   Thread t0 = new Thread(p0);
	   Thread t1 = new Thread(p1);
	   Thread t2 = new Thread(p2);
	   Thread t3 = new Thread(p3);
	   Thread t4 = new Thread(p4);
	   Thread t5 = new Thread(p5);

	   long startTime = System.currentTimeMillis();
	   dropPosgresqlTable();
	   createPosgresqlTable();
	   /*--Run thread--*/
	   t0.start();
	   t1.start();
	   t2.start();
	   t3.start();
	   t4.start();
	   t5.start();
	   
	   /*--Wait for finish--*/
	   try {
		   t0.join(); 
           t1.join(); 
           t2.join(); 
           t3.join(); 
           t4.join();
           t5.join();
       } catch (InterruptedException e) {
    	   System.out.println("Error");
       }	  
	   long endTime = System.currentTimeMillis();
	   long PosgresqlInsertTime = endTime - startTime;
	   /*---------------------Postgresql-------------------------*/
	   
	   
	   /*---------------------Mongodb-------------------------*/
	   startTime = System.currentTimeMillis();
	   /*--Create thread--*/
	   t0 = new Thread(m0);
	   t1 = new Thread(m1);
	   t2 = new Thread(m2);
	   t3 = new Thread(m3);
	   t4 = new Thread(m4);
	   t5 = new Thread(m5);
	   /*--Run thread--*/
	   t0.start();
	   t1.start();
	   t2.start();
	   t3.start();
	   t4.start();
	   t5.start();
	   /*--Wait for finish--*/
	   try {
		   t0.join(); 
		   t1.join();
		   t2.join();
		   t3.join();
		   t4.join();
		   t5.join();
       } catch (InterruptedException e) {
    	   System.out.println("Error");
       }	  	   	   
	   endTime = System.currentTimeMillis();
	   long MongodbInsertTime = endTime - startTime;
	   /*---------------------Mongodb-------------------------*/
	   /*----Result----*/
	   System.out.println("Posgresql Insert Time :　"+PosgresqlInsertTime/1000+"."+PosgresqlInsertTime%1000+"s");
	   System.out.println("Mongodb Insert Time :　"+MongodbInsertTime/1000+"."+MongodbInsertTime%1000+"s");
	   long posgresqlQueryTime=posgresqlQueryTime();
	   System.out.println("Posgresql Query Time :"+posgresqlQueryTime/1000+"."+posgresqlQueryTime%1000+"s");
	   long mongodbQueryTime = mongodbQueryTime();
	   System.out.println("Mongodb Query Time :"+mongodbQueryTime/1000+"."+mongodbQueryTime%1000+"s");
	   System.out.println("End");
   }
}