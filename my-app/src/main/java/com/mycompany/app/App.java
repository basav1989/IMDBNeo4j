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

        public void updatePersonsProperty(final Integer threshold, final Integer batchSize) {
            int  i = 0;
            // use array for batch updates with UNWIND clause
            final List<Map<String, Object>> batches = new ArrayList<>();

            try {
                while (personCursor != null) {

                    i = i + 1;
                    if (i < threshold) {

                        continue;
                    }

                    personCursor = personReader.readLine();
                    String person = "";
                    String birthYear  = "";
                    String roles  = "";                

                    if (personCursor == null) {
                        continue;
                    }

                    String[] tokens = personCursor.split("\t");
                    person = tokens[1];
                    birthYear = tokens[2];
                    roles = tokens[4];

		    // go ahead, create a relation in graph now
		    System.out.println("person  " + person + " born in " + birthYear + " has played roles " + roles);
		    Map<String, Object> map = new HashMap<String, Object>();
                    map.put("person", person);
                    map.put("bornOn", birthYear);
                    map.put("roles", roles);


                    if (batches.size() < batchSize) {
//                        System.out.println("creating batch " + Integer.toString(i));
                        batches.add(map);
                        continue;
                    }

		        // persist the attributes into knowledge graph
		        session.writeTransaction(new TransactionWork() {
						@Override
						public String execute(Transaction tx) {
							System.out.println("executing batch person update transaction");
							// check if node exists with label and properties
							
							// create a parameter map for query
							StatementResult result = null;
                                                        Map<String, Object> parameters = new HashMap<String, Object>();
                                                        parameters.put("batches", batches);
							try {
								// StatementResult result = tx.run("MATCH (a:Person {name:'" + "$nam" + "'}), (b:Movie {name:'" + "$movi" + "'}) RETURN EXISTS((a)-[: " +  relation + "]->(b))", paramMap);
								//result = tx.run("MATCH (a:Person {name:'" + person + "'}), (b:Movie {name:'" + movie + "'}) RETURN EXISTS((a)-[: " +  relation + "]->(b))");
								//if (result.list().size() > 0) {
								//	System.out.println("relation already exists");
								//	return "";
								//}

								// String query = "MATCH (a: Person{name:'" + "$nam" + "'}),(b: Movie{name:'" + "$movi" + "'}) CREATE ((a)-[r: " +  relation + "]->(b)) RETURN r";
                                                                System.out.println("executing batch unwind command");
                                                                result = tx.run( "UNWIND {batches} as batch MERGE (a: Persons {name: batch.person}) SET a.bornOn=batch.bornOn,a.roles=batch.roles", parameters);

								//String query = "MATCH (a: Person{name:'" + person + "'}) MATCH (b: Movie{name:'" + movie + "'}) MERGE ((a)-[r: " +  relation + "]->(b)) RETURN r";
								//System.out.println("executing " + query);
								//result = tx.run(query, paramMap);
							} catch (Exception e) {
                                                                batches.clear();
								e.printStackTrace();
								return "";
							}
							return ""; //result.toString();//single().get(0).toString();
						}
					});
                            System.out.println("clearing batch");
                            batches.clear();
                            System.gc();
                }
	    } catch (Exception n) {
		    n.printStackTrace();
	    }
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
