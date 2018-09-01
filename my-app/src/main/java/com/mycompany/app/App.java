package com.mycompany.app;

/**
 * Hello world!
 *
 */
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import java.util.*;
import java.io.*;
import static org.neo4j.driver.v1.Values.parameters;

public class App implements AutoCloseable 
{
    private final Driver driver;
	private final Session session;
    ArrayList<String> movieDump = new ArrayList<String>(1000);
	BufferedReader reader = null;
	String row = null;
	public App( String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
		session = driver.session();
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

	public void bootstrapIMDB() throws IOException {
		String movieTitlePath = "/home/basavaraj.r/upstream/datasets/name.basics.tsv/data.tsv";
		reader = new BufferedReader(new FileReader(movieTitlePath));
		row  = reader.readLine();
		/*
		if (reader != null) {
			String line = reader.readLine();
			
			while (line != null) {
				String[] tokens = line.split("\t");
				movieDump.add(tokens[2]);
				
				// move to next line
				line = reader.readLine();
			}
		}
		*/
	}

	public void insertPersons()  {
	
		//reader = new BufferedReader(new FileReader("/home/basavaraj.r/upstream/datasets/name.basics.tsv/data.tsv"));
		//String row = reader.readLine();
		int i = 0;
		while (row != null) {
			try {
	        row = reader.readLine();
			String[] tokens  = row.split("\t");
			
			final String person = tokens[1];
			final String birthYear = tokens[2];
			final String deathYear = tokens[3];
			
			System.out.println("inserting " + person + "with id " + tokens[0]);
			
			// create a write transaction and update knowledge graph
			session.writeTransaction(new TransactionWork() {
				@Override
				public String execute(Transaction tx) {
					System.out.println("executing transaction");
					// check if node exists with label and properties
					StatementResult result = tx.run("MATCH (a:Person) WHERE a.name =" + "$name" + " " + "RETURN a", parameters("name", person));
					if (result.list().size() > 0) {
						System.out.println("person already exists");
						return "";
					}
					
					// create a new node with properties and label				
					Integer birth = null;
					Integer death = null;
					
					try {
						birth = Integer.parseInt(birthYear);
					} catch (NumberFormatException n) {
							System.out.println("number format exception for birth");
					}
					
					try {
						death = Integer.parseInt(deathYear);
					} catch (NumberFormatException n) {
						System.out.println("number format exception for death");
					}
					
					if ( (birth != null) && (death != null)) {
						result = tx.run("CREATE (a:Person {name: '" +  person + "' , bornIn: '" + birth + "', diedOn: '" + death + "' }) " + " RETURN a");
						// result = tx.run("CREATE (a:Person {name: $name, bornIn: $birthY, diedOn: $deathY}) " + " RETURN a", parameters("name", person), parameters("birthY", birth), parameters("deathY", death));
					} else if (birth != null) {
						result = tx.run("CREATE (a:Person {name: '" + person + "' , bornIn: '" + birth + "' }) " + " RETURN a"); 
						// result = tx.run("CREATE (a:Person {name: $name, bornIn: $birthY}) " + " RETURN a", parameters("name", person), parameters("birthY", birth));
					}
    				return result.single().get(0).toString();
				}
			});
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void insertMovies(List<String> movies) {
		for (final String movie: movies) {
			System.out.println("adding " + movie);
			session.writeTransaction(new TransactionWork() {
				@Override
				public String execute(Transaction tx) {		
					// check if node exists with label and properties
					StatementResult result = tx.run("MATCH (a:Movie) WHERE a.name =" + "$name" + " " + "RETURN a", parameters("name", movie));
					if (result.list().size() > 0) {
						System.out.println("movie already exists");
						return "";
					}

					// create a new node with properties and label
					result = tx.run("CREATE (a:Movie) " + "SET a.name = $name" + " RETURN a", parameters("name", movie));
					return result.single().get(0).toString();//id();//.asString();
				}
			});
		}
	}

	public void triggerBatchInsert(String entityName) {


		List<String> dataset = null;
		
		// use the correct dataset
		switch (entityName) {
			case "movies":
				dataset = movieDump;
				insertMovies(dataset);
				break;
			case "persons":
				insertPersons();
			default:
				System.out.println("");
		}
		return;
	}
	
    public static void main(String... args ) throws Exception
    {
        try ( App backend = new App( "bolt://localhost", "neo4j", "halkarni89" ) )
        {
            // backend.printGreeting( "hello, world" );
			// List l = new ArrayList<String>();
			// l.add("tom");
			// l.add("hanks");
			// backend.insertActors(l);
			backend.bootstrapIMDB();
			backend.triggerBatchInsert("persons");
        }
    }

}
