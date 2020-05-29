package MavenProject.Ignite;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
public class PersonStoreExample {
	private static final String Driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String JDBC_URL = "jdbc:derby:Ignite;create=true";
    public static void main(String[] args) throws IgniteException, SQLException {
    	DB db = new DB();
    	//defines grid's run time configuration by taking the default parameters
    	IgniteConfiguration cfg = new IgniteConfiguration();
    	// The node will be started as a client node.
        cfg.setClientMode(true);
        // Classes of custom Java logic will be transferred over the wire from this app.
        cfg.setPeerClassLoadingEnabled(true);
        // Setting up an IP Finder to ensure the client can locate the servers.
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
        // Starting the node
        try (Ignite ignite = Ignition.start(cfg)) {
            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache("personCache")) {
                // Load cache with data from the database.
            	cache.clear();
                cache.loadCache(null);
                // Execute query on cache.
                AutoCloseable cursor = cache.query(new SqlFieldsQuery(
                        "select * from Person1 "));
                System.out.println(((QueryCursor<java.util.List<?>>) cursor).getAll());
                //update data in DB
                updateData(cache);
                
                //inserting data after sometime;
                insertData(cache);
                ignite.compute(ignite.cluster().forServers()).broadcast(new RemoteTask());
            }
        }
        
    } 
	 
    private static void insertData(IgniteCache<Long, Person> cache) throws SQLException {
    	Person p = new Person();
    	Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
		stmt = conn.createStatement();
		int queriesCount=50;
		int id = 5500,org = 1833;
		int sal = 2000;
		for(int i=0;i<queriesCount;i++) {
			p.id = id+10;
			p.orgId = org;
			p.salary = sal;
			String name = "Person"+id;
			p.name = name; 
			PreparedStatement ps = conn.prepareStatement("INSERT INTO Person1 (id,orgId, name, salary) VALUES (?, ?,?,?)");
			ps.setInt(1,id);
			ps.setInt(2,org);
			ps.setString(3,name);
			ps.setInt(4,sal);
			ps.execute();
			sal+=100;org++;id++;
			cache.getAndPutIfAbsent((long) id, p);
			trigger(cache,id);
		}
	}
	private static void trigger(IgniteCache<Long, Person> cache,long id) {
		// TODO Auto-generated method stub
		System.out.println(">>>>Value inserted in database");
        SqlFieldsQuery sql  = new SqlFieldsQuery("select * from Person1 where id = ?").setArgs(id);
        System.out.println(id);
        List<List<?>> res = cache.query(new SqlFieldsQuery(sql).setDistributedJoins(true)).getAll();
        for (Object next : res)
            System.out.println(">>>     " + next);
		
	}
	private static void selectDataFromCache(IgniteCache<Integer, Person> cache) {
		// TODO Auto-generated method stub
    	long startS = 0,endS= 0,select =0;
    	startS=System.nanoTime();
    	String sql  = "select * from Person ";
            List<List<?>> res = cache.query(new SqlFieldsQuery(sql).setDistributedJoins(true)).getAll();
            for (Object next : res)
                System.out.println(">>>     " + next);
            endS=System.nanoTime();
     		select = endS  - startS;
     		System.out.println("Time taken for select query in nano seconds "+ select);
		
	}
	private static void updateData(IgniteCache<Long, Person> cache ) throws SQLException {
		Connection conn = null ;
		Statement stmt = null;
		ResultSet rs = null;
		conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
		stmt = conn.createStatement();
		int queriesCount=10;
		int sal = 1000;
		for(int i=0;i<queriesCount;i++) {
			String query = "UPDATE Person1 set salary = ? where id = ?";			
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setInt(1,sal);
			sal+=100;
			ps.setInt(2,i);
			updateDatainCache(cache,sal,i);
		}
			
	}
	//Update query
    private static void updateDatainCache(IgniteCache<Long, Person> cache,int sal,int i) {
    	System.out.println(">>>>Value updated in database");
    	cache.query(new SqlFieldsQuery("UPDATE Person1 SET salary = ? WHERE id = ?").setArgs(sal,i));
    	AutoCloseable cursor = cache.query(new SqlFieldsQuery(
                "select * from Person1 where id = ?").setArgs(i));
        System.out.println(((QueryCursor<java.util.List<?>>) cursor).getAll());
      
    }
    //running in remote server
	private static class RemoteTask implements IgniteRunnable {
       
		
		@IgniteInstanceResource
        Ignite ignite;
        @Override public void run() 
        
        {
            System.out.println(">> Executing the compute task in remote servers ");
            IgniteCache<Long, Person> cache = ignite.getOrCreateCache("personCache");
            AutoCloseable cursor = cache.query(new SqlFieldsQuery(
                    "select * from Person1"));
            System.out.println(((QueryCursor<java.util.List<?>>) cursor).getAll());
    }
    }
}
