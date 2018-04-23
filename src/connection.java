import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;


public class connection {

	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		try 
		{
			String DB_URL = "jdbc:mysql://localhost:3306/sys";
			String USER = "jtod98";
			String PASSWORD = "Password01";
			Connection MyConn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
			System.out.println("Connected");
			
			delete(MyConn);
			create(MyConn);
			//read();
			insert_csv(MyConn);
			//create1(MyConn);
			//insert(MyConn);
			select(MyConn);
			//metaData(MyConn);
            
            MyConn.close();
		} 
		catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		
		catch (FileNotFoundException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void delete(Connection MyConn) throws SQLException
	{		
		//Delete existing table
		Statement drop_stmt = MyConn.createStatement();
		String drop = "DROP TABLE TEST_TABLE";
		drop_stmt.executeUpdate(drop);
		System.out.println("Deleted table in given database");		
	}
	
	public static void create(Connection MyConn) throws SQLException
	{
		//Create new table
		Statement stmt = MyConn.createStatement();            
        String create = "CREATE TABLE TEST_TABLE " +
                     	" (id INTEGER not NULL, " + 
                     	" Name VARCHAR(255), " +
                     	" Age INTEGER," +
                     	" PRIMARY KEY ( id ))";                     
        stmt.executeUpdate(create);       
        System.out.println("Created table in given database");
	}
	
	public static void read() throws FileNotFoundException
	{
		String FILE_NAME = "doc/data.csv";
		File file = new File(FILE_NAME);		
		file.canRead();
				
		Scanner inputStream = new Scanner(file);
		inputStream.nextLine(); // skip column titles
		while(inputStream.hasNext())
		{
			String data = inputStream.nextLine();			
			String[] values = data.split(",");			
			System.out.println("ID: " + values[0] + " Name: " + values [1] + " Age: " + values [2] + "\n");
		}
		inputStream.close();
	}
	
	
	public static void insert_csv(Connection MyConn) throws SQLException, FileNotFoundException
	{
		Statement stmt = MyConn.createStatement();
		String FILE_NAME = "doc/data.csv";
		File file = new File(FILE_NAME);
		file.canRead();
		
		Scanner inputStream = new Scanner(file);
		inputStream.nextLine(); // skip column titles
		
		while(inputStream.hasNext())
		{
			String data = inputStream.nextLine();
			String[] values = data.split(",");
			int id = Integer.parseInt(values[0]);
			String Name = values[1];
			int Age = Integer.parseInt(values[2]);			
			
			String insert = "INSERT INTO TEST_TABLE VALUES('"+id+"' , '"+Name+"' , '"+Age+"' )";							
			stmt.executeUpdate(insert);
		}
		inputStream.close();
		
		System.out.println("Inserted records to given database");
	}
	
	
	public static void insert(Connection MyConn) throws SQLException
	{
		Statement stmt = MyConn.createStatement();
		int id = 100;
		String Name = "Jordan";
		int Age = 19;
		String insert = "INSERT INTO TEST_TABLE " +
						"VALUES ('"+id+"' , '"+Name+"' , '"+Age+"' )";
		stmt.executeUpdate(insert);
		System.out.println("Inserted records to given database");
	}
	
	public static void select(Connection MyConn) throws SQLException
	{
		Statement stmt = MyConn.createStatement();
		String select = "SELECT id, Name, Age FROM TEST_TABLE WHERE Age = '" + 19 + "'" ;
		ResultSet rs = stmt.executeQuery(select);
		
		while(rs.next())
		{
				int id = rs.getInt("id");
				String Name = rs.getString("Name");
				int Age = rs.getInt("Age");
				
				System.out.print("ID: " + id);
				System.out.print(", Name: " + Name);
				System.out.print(", Age: " + Age + "\n");		
		}		
		rs.close();
	}
	
	public static void create1(Connection MyConn) throws SQLException
	{
		//Create new table
		Statement stmt = MyConn.createStatement();
		String create = "CREATE TABLE TEST_TABLE (id INTEGER not NULL, Name VARCHAR(255), Age INTEGER, PRIMARY KEY (id))";
		stmt.executeUpdate(create);       
        System.out.println("Created table in given database");
	}
	
	public static void metaData(Connection MyConn) throws SQLException
	{
		Statement stmt = MyConn.createStatement();
		String meta = ("SELECT id, Name, Age FROM TEST_TABLE");
		ResultSet rs = stmt.executeQuery(meta);
		ResultSetMetaData rsMetaData = rs.getMetaData();
		
		int columnCount = rsMetaData.getColumnCount();
		System.out.println("Column Count: " + columnCount + "\n");
		
		
	}

}
