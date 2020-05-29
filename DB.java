package MavenProject.Ignite;
import java.sql.*;

import org.apache.ignite.cache.query.SqlFieldsQuery;

public class DB {
	private static final String Driver = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String JDBC_URL = "jdbc:derby:Ignite;create=true";
	
	static Connection conn = null ;
	static Connection conn1 = null;
	Statement stmt = null;
	ResultSet rs = null;
	public DB() throws SQLException{
		//this.conn = DriverManager.getConnection(JDBC_URL);
		conn1 = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
		if(this.conn1!=null)
		System.out.println("Connection Successful");
		else
			System.out.println("Connection failed");
		stmt = conn1.createStatement();
		stmt.execute("DROP TABLE Person1");
		stmt.execute("CREATE TABLE Person1 ( id LONG PRIMARY KEY, orgId LONG,name VARCHAR ,salary INT)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (1, 101,'John Doe', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (2, 102,'Jane Roe', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (3,103, 'Mary Major', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (4, 104,'Richard Miles', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (5, 105,'Stark', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (6, 106,'Roy', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (7,107, 'Mary Anne', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (8, 108,'John Richard', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (9, 109,'Johnny', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (10,110,'Jane Roe', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (11,111, 'Mary Major', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (12,112,'Richard Miles', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (13, 113,'Peter', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (14,114,'Ruby', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (15,115, 'Sana', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (16,116,'Sara', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (17, 117,'Black', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (18, 118,'White', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (19,119, 'Brown', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (20, 120,'Danny', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (21,121, 'Mary Peter', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (22,122,'Richy', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (23, 123,'Pentick', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (24,124,'Rold', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (25,125, 'Shals', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (26,126,'Sam', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (27, 127,'Bond', 3500)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (28, 128,'Walter', 2000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (29,129, 'Benny', 1000)");
		stmt.execute("INSERT INTO Person1 (id,orgId, name, salary) VALUES (30, 130,'Doe', 2000)");
		int x = 100,sal=1000;
		for(int i=31;i<100;i++) {
			x +=i;
			PreparedStatement ps = conn1.prepareStatement("INSERT INTO Person1 (id,orgId, name, salary) VALUES (?, ?,?, ?)");
			ps.setInt(1,i);
			ps.setInt(2, x);
			ps.setString(3,"Name"+i);
			ps.setInt(4,sal);
			sal+=100;
			ps.execute();
		}
		
	
	}
	// Register JDBC driver.\
}