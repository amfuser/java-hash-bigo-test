package co.finecode.bigo.pojo;

public class PersonSsn {
	private int id;
	private String ssn;
	
	public PersonSsn() {}
	
	public PersonSsn(int id, String ssn) {
		this.id = id;
		this.ssn = ssn;
	}
	
	/****************
	 * Getters and Setters
	 ****************/
	
	public int getId() {
		return this.id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public String getSsn() {
		return this.ssn;
	}
	
	public void setSsn(String ssn) {
		this.ssn = ssn;
	}
}
