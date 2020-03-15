package co.finecode.bigo;

import java.security.SecureRandom;

/**
 * bigo.java is the class needed to run bigo tests on a large data set. It not only processes the data
 * but builds the data as well
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import co.finecode.bigo.pojo.PersonSsn;

public class Bigo {
	
	private static final Integer BATCH_SIZE = 50000;
	private List<String> pseudoSsns = new ArrayList<String>();
	private Map<Integer,String> ssnMap = new HashMap<Integer,String>();
	private Map<Integer,PersonSsn> ssnPojoMap = new HashMap<Integer,PersonSsn>();
	private List<PersonSsn> personSsns = new ArrayList<PersonSsn>();
	
	private static final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	
	/**
	 * Builds the tables needed for bigo exercise
	 * @param connection
	 */
	
	public void buildTables(Connection connection) {
		String personSql = "create table Person (" +
				"id INT(11) NOT NULL AUTO_INCREMENT," +
				"first_name VARCHAR(20)," +
			  "last_name VARCHAR(20)," +
			  "CONSTRAINT id_pk PRIMARY KEY (id)" + 
			  ");";

		String personSsnSql = "create table Person_Ssn (" +
				"id INT(11)," +
				"ssn VARCHAR(20) NOT NULL" +
				");";
		
		Statement creatTablesStatement = null;
		try {
			creatTablesStatement = connection.createStatement();
			creatTablesStatement.executeUpdate(personSql);
			creatTablesStatement.executeUpdate(personSsnSql);
			creatTablesStatement.close();
		}
		catch(Exception ex) {
			System.out.println("Failed to create tables");
			ex.printStackTrace();
		}
		finally {
			if(creatTablesStatement != null) {
				try {
					creatTablesStatement.close();
				}
				catch(Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 
	 * @param connection
	 * @param numberOfPersons
	 * @param insertPersons
	 */
	
	public void insertDataIntoTables(Connection connection, int numberOfPersons, boolean insertPersons) {
		int numOfIterations = numberOfPersons / BATCH_SIZE;
		int remainder = numberOfPersons % BATCH_SIZE;
		int numberToInsert = 0;
		if (numOfIterations == 0) {
			numberToInsert = remainder;
			numOfIterations = 1;
		}
		else {
			numberToInsert = BATCH_SIZE;
			if (remainder > 0)
				numOfIterations++;
		}

		PreparedStatement ps;
		try {
			if (insertPersons) {
				String insertPersonSQL = "insert into Person values(0,?,?)";
				ps = connection.prepareStatement(insertPersonSQL);
				for (int i = 0; i < numOfIterations; i++) {
					if (i == (numOfIterations - 2) && remainder > 0)
						numberToInsert = remainder;
					for (int j = 0; j < numberToInsert; j++) {
						String[] name = createPersonName();
						String fName = name[0];
						String lName = name[1];
						ps.setString(1, fName);
						ps.setString(2, lName);
						ps.addBatch();
					}
					ps.executeBatch();
				}
			}
			else {
				String insertPersonSsnSQL = "insert into Person_Ssn values(?,?)";
				ps = connection.prepareStatement(insertPersonSsnSQL);
				int id = 0;
				for (int i = 0; i < numOfIterations; i++) {
					if (i == (numOfIterations - 2) && remainder > 0)
						numberToInsert = remainder;
					for (int j = 0; j < numberToInsert; j++) {
						String ssn = createUniqueSsn();
						ps.setInt(1, ++id);
						ps.setString(2, ssn);
						ps.addBatch();
					}
					ps.executeBatch();
				}
			}
		}
		catch (Exception ex) {
			System.out.println("Person data insert(s) failed");
			ex.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param connection
	 */
	
	public void loadDataMaps(Connection connection) {
		// No need for the list anymore so free it up
		pseudoSsns = new ArrayList<String>();
		
		String selectSsnSql = "select id, ssn from Person_Ssn";
		String selectPersonsSql = "select id, first_name, last_name from Person";
		try {
			Date start = new Date();
			System.out.println(start.toString());
			Statement statement1 = connection.createStatement();
			ResultSet rs1 = statement1.executeQuery(selectSsnSql);
			while (rs1.next()) {
				Integer id = rs1.getInt("id");
				String ssn = rs1.getString("ssn");
				ssnMap.put(id, ssn);
			}
			
			statement1.close();
			rs1.close();
			
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectPersonsSql);
			while (rs.next()) {
				Integer id = rs.getInt("id");
				String fname = rs.getString("first_name");
				String lname = rs.getString("last_name");
				String ssn = ssnMap.get(id);
				pseudoSsns.add(fname + " " + lname + ", " + ssn);
			}
			Date end = new Date();
			System.out.println(end.toString());
			System.out.println(pseudoSsns.size() + " Records Loaded.");
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param connection
	 */
	
	public void loadDataLambda(Connection connection) {
		pseudoSsns = new ArrayList<String>();
		
		String selectSsnSql = "select id, ssn from Person_Ssn";
		String selectPersonsSql = "select id, first_name, last_name from Person";
		try {
			Date start = new Date();
			System.out.println(start.toString());
			Statement statement1 = connection.createStatement();
			ResultSet rs1 = statement1.executeQuery(selectSsnSql);
			while (rs1.next()) {
				Integer id = rs1.getInt("id");
				String ssn = rs1.getString("ssn");
				PersonSsn personSsn = new PersonSsn(id, ssn);
				personSsns.add(personSsn);
			}
			
			statement1.close();
			rs1.close();
			
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectPersonsSql);
			while (rs.next()) {
				Integer id = rs.getInt("id");
				String fname = rs.getString("first_name");
				String lname = rs.getString("last_name");
				PersonSsn personSsn = new PersonSsn();
				personSsn = personSsns.stream().filter(x -> id == x.getId()).findAny().orElse(null);
				pseudoSsns.add(fname + " " + lname + ", " + personSsn.getSsn());
			}
			Date end = new Date();
			System.out.println(end.toString());
			System.out.println(pseudoSsns.size() + " Records Loaded.");
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param connection
	 */
	
	public void loadDataPojoMap(Connection connection) {
		pseudoSsns = new ArrayList<String>();
		
		String selectSsnSql = "select id, ssn from Person_Ssn";
		String selectPersonsSql = "select id, first_name, last_name from Person";
		try {
			Date start = new Date();
			System.out.println(start.toString());
			Statement statement1 = connection.createStatement();
			ResultSet rs1 = statement1.executeQuery(selectSsnSql);
			while (rs1.next()) {
				Integer id = rs1.getInt("id");
				String ssn = rs1.getString("ssn");
				PersonSsn personSsn = new PersonSsn(id, ssn);
				ssnPojoMap.put(id, personSsn);
			}
			
			statement1.close();
			rs1.close();
			
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(selectPersonsSql);
			while (rs.next()) {
				Integer id = rs.getInt("id");
				String fname = rs.getString("first_name");
				String lname = rs.getString("last_name");
				PersonSsn personSsn = new PersonSsn();
				personSsn = ssnPojoMap.get(id);
				pseudoSsns.add(fname + " " + lname + ", " + personSsn.getSsn());
			}
			Date end = new Date();
			System.out.println(end.toString());
			System.out.println(pseudoSsns.size() + " Records Loaded.");
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	
	/**
	 * 
	 * @param connection
	 */
	
	public void dropTables(Connection connection) {
		
	}
	
	/**
	 * Generates a random first and last name and returns them in an array.
	 * @return
	 */
	
	private String[] createPersonName() {
		String[] name = new String[2];
		Random ran = new Random();
		// get first name
		int length = ran.nextInt(3) + 6;
		String firstName = generateRandomName(length);
		// get last name
		length = ran.nextInt(3) + 6;
		String lastName = generateRandomName(length);
		name[0] = firstName;
		name[1] = lastName;
		return name;
	}
	
	/**
	 * Returns a unique sudo ssn
	 * @return
	 */
	
	private String createUniqueSsn() {
		// This code courtesy of Stack Overflow:
		// https://stackoverflow.com/questions/12659572/how-to-generate-a-random-9-digit-number-in-java
		String ssn = "";
		boolean isUnique = false;
		
		while(!isUnique) {
			long timeSeed = System.nanoTime(); 
			double randSeed = Math.random() * 1000; 

			long midSeed = (long) (timeSeed * randSeed); 
			String s = midSeed + "";
			String subStr = s.substring(0, 9);
			if(!pseudoSsns.contains(subStr)) {
				isUnique = true;
				ssn = subStr;
			}
		}
    

    return ssn;
	}

	/**
	 * Courtesy of stack overflow:
	 * https://stackoverflow.com/questions/39222044/generate-random-string-in-java
	 * @param length
	 * @return
	 */
	
	public String generateRandomName(int length) {
		Random random = new SecureRandom();
		if (length <= 0) {
			throw new IllegalArgumentException("String length must be a positive integer");
		}

		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(characters.charAt(random.nextInt(characters.length())));
		}

		return sb.toString();
	}
}
