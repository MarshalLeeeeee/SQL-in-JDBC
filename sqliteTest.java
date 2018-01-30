package sqliteTest;

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
			ResultSet rs =  stat.executeQuery("select avg(runtime) as avg from movies where year = 1963;");
			while (rs.next()) { 
				System.out.println("average runtime = " + rs.getString("avg"));  
	          }  
			rs.close();
			conn.commit();
    	}catch( Exception e ){  
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
    	System.out.println("Thread1(avg) over with runtime = " + (System.currentTimeMillis()-starttime));
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
			for(int i = 0; i < 100000; i++){
				int rand_runtime = rand.nextInt(199) + 1;
				sql = "INSERT INTO movies VALUES('INS" + i + "','long',1963," + rand_runtime + ")";
				stat.addBatch(sql);
			}
			stat.executeBatch();
			conn.commit();
    	 }catch( Exception e ){  
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
    	System.out.println("Thread2(ins) over with runtime = " + (System.currentTimeMillis()-starttime));
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
    	while(flag){
    		try {
    			Statement stat = conn.createStatement();
    			rs =  stat.executeQuery("select avg(runtime) as avg from movies where id in (select movie_id from ratings where rating > 6);");
    			while (rs.next()) { 
    				System.out.println("average runtime = " + rs.getString("avg"));  
    	          }  
    			conn.commit();
        	}catch( Exception e ){  
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
    	System.out.println("Thread3(avg) over with runtime = " + (System.currentTimeMillis()-starttime));
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
    	boolean flag = true;
    	while(flag){
    		try{
    			Statement stat = conn.createStatement();
    			String sql = "delete from movies where year in (2016,2017) and id in (select movie_id from ratings where rating > 6);";
    			line = stat.executeUpdate(sql);
    			conn.commit();
        	}catch( Exception e ){  
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
    	System.out.println("Thread4(del) over with runtime = " + (System.currentTimeMillis()-starttime));
    }  
}  

public class sqliteTest {  
    public static void main(String[] args) throws ClassNotFoundException {  
    	try {
    		Class.forName("org.sqlite.JDBC");
			Connection conn1 = DriverManager.getConnection("jdbc:sqlite:prob1.db");
			Connection conn2 = DriverManager.getConnection("jdbc:sqlite:prob1.db");
			Connection conn3 = DriverManager.getConnection("jdbc:sqlite:prob2.db");
			Connection conn4 = DriverManager.getConnection("jdbc:sqlite:prob2.db");
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			conn3.setAutoCommit(false);
			conn4.setAutoCommit(false);
			conn3.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			conn4.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			System.out.println("Test in sqlite...\n");
			System.out.println("************************");
			System.out.println("Problem1:");
			System.out.println("Return the average runtime of movies whose release year is 1963.");
			System.out.println("And insert movies infos into the movies table whose year is 1963.\n");
    		Thread1 thread1=new Thread1("A",conn1);  
    		Thread2 thread2=new Thread2("B",conn2);  
    		thread1.start();  
    		thread2.start();  
    		thread1.join();
    		thread2.join();
    		System.out.println("************************");
    		System.out.println("Problem2:");
			System.out.println("Return the average runtime of movies whose rating is over 6.0");
			System.out.println("And delete movies from the table whose rating is over 6.0 and release year is 2016 and 2017.\n");
			Thread3 thread3=new Thread3("C",conn3);  
    		Thread4 thread4=new Thread4("D",conn4);  
    		thread3.start();  
    		thread4.start();  
    		thread3.join();
    		thread4.join();
			System.out.println("\nThe test program for sqlite is over.....");
    	}
    	 catch( Exception e )  
        {  
            e.printStackTrace ( );  
        }  
    }  

}  


