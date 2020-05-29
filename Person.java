package MavenProject.Ignite;
public class Person {
	long id;
    long orgId;
    String name;
    int salary;
    public Person(long long1, long long2, String string, int int1) {
    	this.id = long1;
    	this.orgId = long2;
    	this.name = string;
    	this.salary = int1;
	}
	public Person() {
	}
	public Long getId() {
		return this.id;
	}
	
    
}