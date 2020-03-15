package co.finecode.bigo;

import java.sql.Connection;
import java.sql.DriverManager;

public class Main {
	
	/**
	 * Main Method
	 * @param args
	 */
	
	public static void main(String args[]) {
		Connection connection = null;
		try {
			connection = getDbConnection(args);
			Bigo bigo = new Bigo();
			bigo.buildTables(connection);
			bigo.insertDataIntoTables(connection, 1000000, true);
			bigo.insertDataIntoTables(connection, 1000000, false);
			bigo.loadDataMaps(connection);
			//bigo.loadDataLambda(connection);
			//bigo.loadDataPojoMap(connection);
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}
		finally {
			try {
				if(connection != null)
					connection.close();
			}
			catch(Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * Returns a database connection object
	 * @param args
	 * @return
	 */
	
	public static Connection getDbConnection(String[] args) {
		try {
			Class.forName("org.mariadb.jdbc.Driver");
		}
		catch (ClassNotFoundException e) {
			System.out.println("Driver not found");
			e.printStackTrace();
		}

		Connection connection = null;

		try {
			String connectionString = "";
			String ipAddress = args[0].trim();
			String dbName = args[1].trim();
			String login = args[2].trim();
			String password = "";
			if (args.length == 4)
				password = args[3].trim();
			connectionString = "jdbc:mysql://" + ipAddress + ":3306/" + dbName + "?user=" + login + "&password=" + password;
			connection = DriverManager.getConnection(connectionString);
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}

		return connection;
	}
}
