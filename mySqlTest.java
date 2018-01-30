package mySqlTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

class Thread1 extends Thread{  
	private String name;  
	private Connection conn;
	public Thread1(String name,Connection conn) {  
		this.name=name; 
		this.conn=conn;
	    }  
    public void run() {
    	Statement stat = null;
    	long starttime = System.currentTimeMillis();
    	try {
			stat = conn.createStatement();
			//stat.execute("SET autocommit = 0;");
			System.out.println("Thread " + this.name + " starts...");
			//stat.execute("LOCK TABLES movies READ;");
			//ResultSet rs =  stat.executeQuery("select avg(runtime) as avg from movies where year = 1963 ;");
			ResultSet rs =  stat.executeQuery("select avg(runtime) as avg from movies where year = 1963 lock in share mode;");
			//ResultSet rs =  stat.executeQuery("select avg(runtime) as avg from movies where year = 1963 for update;");
			while (rs.next()) { 
				System.out.println("average runtime = " + rs.getString("avg"));  
	        } 
			//stat.execute("UNLOCK TABLES;");
			conn.commit();
			rs.close();
    	}catch( Exception e ){  
    		System.out.println("Thread " + this.name + " throws exception");
           e.printStackTrace ( ); 
           try{
        	   conn.rollback();
           }catch(Exception ex){
        	   ex.printStackTrace();
           }
    	}finally{
    		if(stat != null){
    			try{
    				stat.close();
    			}catch(Exception exx){
    				exx.printStackTrace();
    			}
    		}
    	}
    	System.out.println("Thread " + this.name + " (avg) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}  

class Thread2 extends Thread{  
    private String name;  
    private Connection conn;
    public Thread2(String name,Connection conn) {  
       this.name=name;  
       this.conn=conn;
    }  
    public void run() { 
    	long starttime = System.currentTimeMillis();
    	Statement stat = null;
        try {
			stat = conn.createStatement();
			Random rand = new Random();
			String sql = null;
			//stat.execute("SET autocommit = 0;");
			System.out.println("Thread " + this.name + " starts...");
			//stat.execute("LOCK TABLES movies WRITE;");
			for(int i = 0; i < 100000; i++){
				int rand_runtime = rand.nextInt(199) + 1;
				sql = "INSERT INTO movies VALUES('INSEwRT" + i + "','long',1963," + rand_runtime + ")";
				//stat.addBatch(sql);
				stat.executeUpdate(sql);
				conn.commit();
			}
			//stat.executeBatch();
			//stat.execute("UNLOCK TABLES;");
			//conn.commit();
    	 }catch( Exception e ){  
    		 System.out.println("Thread " + this.name + " throws exception");
           e.printStackTrace ( ); 
           try{
        	   conn.rollback();
           }catch(Exception ex){
        	   ex.printStackTrace();
           }
    	 } finally{
    		 if(stat != null){
    			 try{
    				 stat.close();
    			 }catch(Exception exx){
    				 exx.printStackTrace();
    			 }
    		 }
    	 }
    	System.out.println("Thread " + this.name + " (ins) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}

class Thread3 extends Thread{  
	private String name;  
	private Connection conn;
	public Thread3(String name,Connection conn) {  
		this.name=name; 
		this.conn=conn;
	    }  
    public void run() {
    	long starttime = System.currentTimeMillis();
    	boolean flag = true;
    	ResultSet rs = null;
    	Statement stat;
    	while(flag){
    		try {
    			stat = conn.createStatement();
    			System.out.println("Thread " + this.name + " starts to calculate for the average...");
    			//stat.execute("LOCK TABLES movies READ, ratings READ;");
    			rs = stat.executeQuery("SELECT avg(runtime) as avg " +
                        "FROM movies WHERE id IN " +
                        "(SELECT DISTINCT movie_id FROM ratings WHERE rating > 6.0) for update;");
    			while (rs.next()) { 
    				System.out.println("average runtime = " + rs.getString("avg"));  
    	          }  
    			//stat.execute("UNLOCK TABLES;");
    			conn.commit();
        	}catch( Exception e ){  
        		System.out.println("Thread " + this.name + "  throws exception");
               e.printStackTrace ( ); 
               try{
            	   conn.rollback();
               }catch(Exception ex){
            	   ex.printStackTrace();
               }
        	}finally{
        		if(rs != null){
        			try{
        				rs.close();
        			}catch(Exception exx){
        				exx.printStackTrace();
        			}
        			flag = false;
        		}
        	}
    	}
    	System.out.println("Thread " + this.name + " (avg) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}

class Thread4 extends Thread{  
	private String name;  
	private Connection conn;
	public Thread4(String name,Connection conn) {  
		this.name=name; 
		this.conn=conn;
	    }  
    public void run() {
    	long starttime = System.currentTimeMillis();
    	int line = -1;
    	ResultSet rs;
    	boolean flag = true;
    	Statement stat;
    	while(flag){
    		try{
    			stat = conn.createStatement();
    			System.out.println("Thread " + this.name + " starts to delete");
    			//stat.execute("LOCK TABLES movies WRITE, ratings READ;");
    			//try{
    			//	stat.execute("create tempotary table tt2 select movie_id from ratings where rating > 6");
    			//}catch(Exception e){}
    			//String sql = "delete from movies where year in (2016,2017) and id in tt2;";
    			line = //stat.executeUpdate("delete from movies where year in (2016,2017) and id in tt7;");
    					stat.executeUpdate("DELETE FROM movies WHERE year IN (2016,2017) AND id IN" +
                        "(SELECT DISTINCT movie_id FROM ratings WHERE rating > 6.0);");
    			//stat.execute("UNLOCK TABLES;");
    			conn.commit();
        	}catch( Exception e ){  
        		System.out.println("Thread " + this.name + " throws exception");
               e.printStackTrace ( ); 
               try{
            	   conn.rollback();
               }catch(Exception ex){
            	   ex.printStackTrace();
               }
        	 }finally{
        		 if(line != -1) flag = false;
        	 }
    	}
    	System.out.println("Thread " + this.name + " (del) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}  

class Thread5 extends Thread{  
	private String name;  
	private Connection conn;
	public Thread5(String name,Connection conn) {  
		this.name=name; 
		this.conn=conn;
	    }  
    public void run() {
    	long starttime = System.currentTimeMillis();
    	int line = -1;
    	ResultSet rs;
    	boolean flag = true;
    	Statement stat;
    	while(flag){
    		try{
    			stat = conn.createStatement();
    			System.out.println("Thread " + this.name + " starts to recover...");
    			//stat.execute("LOCK TABLES movies WRITE, ratings READ;");
    			//try{
    			//	stat.execute("create tempotary table tt2 select movie_id from ratings where rating > 6");
    			//}catch(Exception e){}
    			//String sql = "delete from movies where year in (2016,2017) and id in tt2;";
    			line = //stat.executeUpdate("delete from movies where year in (2016,2017) and id in tt7;");
    					stat.executeUpdate("INSERT into movies (select * from recovery);");
    			//stat.execute("UNLOCK TABLES;");
    			conn.commit();
        	}catch( Exception e ){  
        		System.out.println("Thread " + this.name + " throws exception");
               e.printStackTrace ( ); 
               try{
            	   conn.rollback();
               }catch(Exception ex){
            	   ex.printStackTrace();
               }
        	 }finally{
        		 if(line != -1) flag = false;
        	 }
    	}
    	System.out.println("Thread " + this.name + " (del) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
} 

class Thread6 extends Thread{  
	private String name;  
	private Connection conn;
	int start_year, end_year;
	public Thread6(int start, int end, String name,Connection conn) {  
		this.name=name; 
		this.conn=conn;
		this.start_year = start;
		this.end_year = end;
	    }  
    public void run() {
    	Statement stat = null;
    	ResultSet rs = null;
    	long starttime = System.currentTimeMillis();
    	String sql;
    	try {
			stat = conn.createStatement();
			//stat.execute("SET autocommit = 0;");
			System.out.println("Thread " + this.name + " starts...");
			stat.execute("LOCK TABLES movies WRITE;");
			//ResultSet rs =  stat.executeQuery("select sum(runtime) as s from movies where " + start_year + " < year < " + end_year + ";");
			System.out.println("Thread " + this.name + " has key...");
			sql = "select sum(runtime) as s from movies where " + start_year + " < year and year < " + end_year;
			for(int i = 0; i < 10; i++){
			rs =  stat.executeQuery(sql);
			}
			//ResultSet rs =  stat.executeQuery("select sum(runtime) as s from movies where year < 2000 for update;");
			while (rs.next()) { 
				System.out.println("sum runtime (" + start_year + "<= year <" + end_year + ") = " + rs.getString("s"));  
	        } 
			stat.execute("UNLOCK TABLES;");
			conn.commit();
			rs.close();
    	}catch( Exception e ){  
    		System.out.println("Thread " + this.name + " throws exception");
           e.printStackTrace ( ); 
           try{
        	   conn.rollback();
           }catch(Exception ex){
        	   ex.printStackTrace();
           }
    	}finally{
    		if(stat != null){
    			try{
    				stat.close();
    			}catch(Exception exx){
    				exx.printStackTrace();
    			}
    		}
    	}
    	System.out.println("Thread " + this.name + " (avg) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}

class Thread7 extends Thread{  
	private String name;  
	private Connection conn;
	public Thread7(String name,Connection conn) {  
		this.name=name; 
		this.conn=conn;
	    }  
    public void run() {
    	Statement stat = null;
    	String sql;
    	long starttime = System.currentTimeMillis();
    	try {
			stat = conn.createStatement();
			//stat.execute("SET autocommit = 0;");
			System.out.println("Thread " + this.name + " starts...");
			//stat.execute("LOCK TABLES movies READ;");
			sql = "select avg(runtime) as avg from movies;";
			//ResultSet rs =  stat.executeQuery("select sum(runtime) as s from movies where year >= 2000 ;");
			ResultSet rs =  stat.executeQuery(sql + "lock in share mode;");
			while (rs.next()) { 
				System.out.println("first read for avg: " + rs.getString("avg"));  
	        } 
			rs =  stat.executeQuery(sql + "lock in share mode;");
			while (rs.next()) { 
				System.out.println("second read for avg: " + rs.getString("avg"));  
	        } 
			//stat.execute("UNLOCK TABLES;");
			conn.commit();
			rs.close();
    	}catch( Exception e ){  
    		System.out.println("Thread " + this.name + " throws exception");
           e.printStackTrace ( ); 
           try{
        	   conn.rollback();
           }catch(Exception ex){
        	   ex.printStackTrace();
           }
    	}finally{
    		if(stat != null){
    			try{
    				stat.close();
    			}catch(Exception exx){
    				exx.printStackTrace();
    			}
    		}
    	}
    	System.out.println("Thread " + this.name + " (avg) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}

class Thread8 extends Thread{  
    private String name;  
    private Connection conn;
    public Thread8(String name,Connection conn) {  
       this.name=name;  
       this.conn=conn;
    }  
    public void run() { 
    	long starttime = System.currentTimeMillis();
    	Statement stat = null;
        try {
			stat = conn.createStatement();
			Random rand = new Random();
			String sql = null;
			//stat.execute("SET autocommit = 0;");
			System.out.println("Thread " + this.name + " starts...");
			//stat.execute("LOCK TABLES movies WRITE;");
			for(int i = 0; i < 100000; i++){
				int rand_runtime = rand.nextInt(199) + 1;
				sql = "INSERT INTO movies VALUES('INSthreadd88" + i + "','long',1963," + rand_runtime + ")";
				//stat.addBatch(sql);
				stat.executeUpdate(sql);
				//conn.commit();
			}
			//stat.executeBatch();
			//stat.execute("UNLOCK TABLES;");
			conn.commit();
    	 }catch( Exception e ){  
    		 System.out.println("Thread " + this.name + " throws exception");
           e.printStackTrace ( ); 
           try{
        	   conn.rollback();
           }catch(Exception ex){
        	   ex.printStackTrace();
           }
    	 } finally{
    		 if(stat != null){
    			 try{
    				 stat.close();
    			 }catch(Exception exx){
    				 exx.printStackTrace();
    			 }
    		 }
    	 }
    	System.out.println("Thread " + this.name + " (ins) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}

class Thread9 extends Thread{  
    private String name;  
    private Connection conn;
    public Thread9(String name,Connection conn) {  
       this.name=name;  
       this.conn=conn;
    }  
    public void run() { 
    	long starttime = System.currentTimeMillis();
    	Statement stat = null;
        try {
			stat = conn.createStatement();
			Random rand = new Random();
			String sql = null;
			//stat.execute("SET autocommit = 0;");
			System.out.println("Thread " + this.name + " starts...");
			//stat.execute("LOCK TABLES movies WRITE;");
			for(int i = 0; i < 100000; i++){
				int rand_runtime = rand.nextInt(199) + 1;
				sql = "INSERT INTO movies VALUES('INSthreadd99" + i + "','long',1963," + rand_runtime + ")";
				//stat.addBatch(sql);
				stat.executeUpdate(sql);
				//conn.commit();
			}
			//stat.executeBatch();
			//stat.execute("UNLOCK TABLES;");
			conn.commit();
    	 }catch( Exception e ){  
    		 System.out.println("Thread " + this.name + " throws exception");
           e.printStackTrace ( ); 
           try{
        	   conn.rollback();
           }catch(Exception ex){
        	   ex.printStackTrace();
           }
    	 } finally{
    		 if(stat != null){
    			 try{
    				 stat.close();
    			 }catch(Exception exx){
    				 exx.printStackTrace();
    			 }
    		 }
    	 }
    	System.out.println("Thread " + this.name + " (ins) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}

public class mySqlTest {  
    public static void main(String[] args) throws ClassNotFoundException {  
    	try {
    		Class.forName("com.mysql.jdbc.Driver");
    		String db_url = "jdbc:mysql://127.0.0.1/prob_copy3";
			Connection conn1 = DriverManager.getConnection(db_url,"root","");
			Connection conn2 = DriverManager.getConnection(db_url,"root","");
			Connection conn3 = DriverManager.getConnection(db_url,"root","");
			Connection conn4 = DriverManager.getConnection(db_url,"root","");
			Connection conn5 = DriverManager.getConnection(db_url,"root","");
			Connection conn6 = DriverManager.getConnection(db_url,"root","");
			Connection conn7 = DriverManager.getConnection(db_url,"root","");
			Connection connPre = DriverManager.getConnection(db_url,"root","");
			Connection connCon = DriverManager.getConnection(db_url,"root","");
			Connection connRcv = DriverManager.getConnection(db_url,"root","");
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			conn3.setAutoCommit(false);
			conn4.setAutoCommit(false);
			conn5.setAutoCommit(false);
			conn7.setAutoCommit(false);
			conn6.setAutoCommit(false);
			connPre.setAutoCommit(false);
			connCon.setAutoCommit(false);
			connRcv.setAutoCommit(false);
			int Iso = Connection.TRANSACTION_REPEATABLE_READ; // by changing iso to set the isolation level
			conn1.setTransactionIsolation(Iso);
			conn2.setTransactionIsolation(Iso);
			conn3.setTransactionIsolation(Iso);
			conn4.setTransactionIsolation(Iso);
			conn5.setTransactionIsolation(Iso);
			conn6.setTransactionIsolation(Iso);
			conn7.setTransactionIsolation(Iso);
			System.out.println("Test in mysql...\n");

			/**
			 * This part is related to the problem 1, 
			System.out.println("************************");
			System.out.println("Problem1:");
			System.out.println("Isolation level: TRANSACTION_SERIALIZABLE"); // this output will change manually according to the isolation level
			System.out.println("Granularity: share lock");                   // this output will change manually according to the lock type
			System.out.println("Return the average runtime of movies whose release year is 1963.");
			System.out.println("And insert movies infos into the movies table whose year is 1963.\n");
			Thread1 threadPre = new Thread1("Pre",connPre);
			Thread1 threadCon = new Thread1("Con",connCon);
    		Thread1 thread1=new Thread1("A",conn1);  
    		Thread2 thread2=new Thread2("B",conn2);
    		System.out.println("The original average number:");
    		threadPre.start();
    		threadPre.join();
    		System.out.println("");
    		thread1.start();  
    		thread2.start();  
    		thread1.join();
    		thread2.join();
    		System.out.println("\nThe current average number:");
    		threadCon.start();
    		threadCon.join();
    		System.out.println("");
    		*/

			/**
			 * This part is related to the problem 2
    		System.out.println("************************");
    		System.out.println("Problem2:");
    		System.out.println("Isolation level: TRANSACTION_READ_COMMITTED"); // this output will change manually according to the isolation level
			System.out.println("Granularity: x lock");						   // this output will change manually according to the lock level
			System.out.println("Return the average runtime of movies whose rating is over 6.0");
			System.out.println("And delete movies from the table whose rating is over 6.0 and release year is 2016 and 2017.\n");
			Thread3 threadPre = new Thread3("Pre",connPre);
			Thread3 threadCon = new Thread3("Con",connCon);
			Thread5 threadRcv = new Thread5("Rcv",connRcv);
			Thread3 thread3 = new Thread3("C",conn3);  
    		Thread4 thread4 = new Thread4("D",conn4);  
    		threadRcv.start();
    		threadRcv.join();
    		System.out.println("The original average number:");
    		threadPre.start();
    		threadPre.join();
    		System.out.println("");
    		thread3.start();  
    		thread4.start();  
    		thread3.join();
    		thread4.join();
    		System.out.println("\nThe current average number:");
    		threadCon.start();
    		threadCon.join();
    		System.out.println("");
    		*/
			System.out.println("************************");
    		System.out.println("Problem3:");
    		System.out.println("Isolation level: TRANSACTION_REPEATABLE_READ");
			System.out.println("Granularity: x lock");
			System.out.println("insert item into movies\n");
			long start_time = System.currentTimeMillis();
			//System.out.println("And delete movies from the table whose rating is over 6.0 and release year is 2016 and 2017.\n");
			/*
			Thread7 mth1 = new Thread7("1",conn1);
			Thread7 mth2 = new Thread7("2",conn2);
			Thread7 mth3 = new Thread7("3",conn3);
			Thread7 mth4 = new Thread7("4",conn4);
			Thread7 mth5 = new Thread7("5",conn5);
			Thread7 mth6 = new Thread7("6",conn6);
			Thread7 mth7 = new Thread7("7",conn7);
			*/
			/**
			 * this part is related to the bonus senario 1
			Thread6 mth1 = new Thread6("1",conn1);
			Thread6 mth2 = new Thread6("2",conn2);
			 * this part is related to the bonus senario 2
			Thread8 mth1 = new Thread8("1",conn1);
			Thread9 mth2 = new Thread9("2",conn2);
			*/
			/*
			Thread6 mth3 = new Thread6(1950,1975,"3",conn3);
			Thread6 mth4 = new Thread6(1975,2000,"4",conn4);
			Thread6 mth5 = new Thread6(2000,2025,"5",conn5);
			Thread6 mth6 = new Thread6(2025,2050,"6",conn6);
			Thread6 mth7 = new Thread6(2050,2075,"7",conn7);
			*/
			mth1.start();
			mth2.start();
			/*
			mth3.start();
			mth4.start();
			mth5.start();
			mth6.start();
			mth7.start();
			*/
			mth1.join();
			mth2.join();
			/*
			mth3.join();
			mth4.join();
			mth5.join();
			mth6.join();
			mth7.join();
			*/
			long end_time = System.currentTimeMillis();
			//System.out.println("The test program for mysql is over.....");
			System.out.println("The test program for mysql is over with running time: " + (end_time - start_time) + ".....");
    	}
    	 catch( Exception e )  
        {  
            e.printStackTrace ( );  
        }  
    }  

}  


