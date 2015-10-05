import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.orientechnologies.orient.core.sql.OCommandSQL;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class DateImporter {
	
	private OrientGraphNoTx graph;
	private OrientGraphFactory factory;
	
	public static void main(String[] args) {

    	if (args.length < 3) {
      		System.err.println("Usage: DB_Manager <inputKeys> <edges> <db_path>");
      		System.exit(1);
    	}
    	String keysFilePath = args[0];
    	String edgesFilePath = args[1];
    	String dbPath = args[2];
    	
    	DateImporter importer = new DateImporter();
    	
    	importer.loadDatabase(dbPath);
    	importer.createClasses();
    	importer.loadVerticesAndEdges(keysFilePath, edgesFilePath);
    	
		
	}
	public void loadDatabase(String databasePath){
		factory = new OrientGraphFactory(databasePath);
		graph = factory.getNoTx(); //.getTx();
		
	}
	public void createClasses(){
		
		
		if(graph.getVertexType("url")==null){
			
		
			graph.createVertexType("url");
			String sql = "CREATE PROPERTY url.name string";
			OCommandSQL createIndex = new OCommandSQL(sql);
			graph.command(createIndex).execute(new Object[0]);
			
			sql = "create index url.name on url (name) unique";
			createIndex = new OCommandSQL(sql);
			Object done = graph.command(createIndex).execute(new Object[0]);
			System.out.println(done.toString());
			
		}
		if(graph.getVertexType("artist")==null){
			graph.createVertexType("artist");
		}
		
		if(graph.getVertexType("artist").getProperty("artist.date")!=null){
			String sql = "CREATE PROPERTY artist.date integer";
			OCommandSQL createIndex = new OCommandSQL(sql);
			graph.command(createIndex).execute(new Object[0]);
		}
		
			String sql = "CREATE PROPERTY artist.name string";
			OCommandSQL createIndex = new OCommandSQL(sql);
			graph.command(createIndex).execute(new Object[0]);
		
		
		sql = "create index artist.name on artist (name) unique";
		createIndex = new OCommandSQL(sql);
		graph.command(createIndex).execute(new Object[0]);
		
	}

	public void loadVerticesAndEdges(String keys, String edges) {
		
		try{
			insertVertices(keys);
			graph.commit();
			
		} catch( Exception e ) {
			e.printStackTrace();
			graph.rollback();

		} finally {
			graph.shutdown();
			factory.close();
		}
		
    }
	
	
	private void insertVertices(String inputFile) throws IOException {
		
		BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));	
		String line = inputReader.readLine();
		
		int date = -1;
		
		while(line!=null){
		
			String artistName = line.split(",")[0];
		
			try{
				date = Integer.parseInt(line.split(",")[1]);
			}catch(Exception e){
				date = 0;
			}finally{

				Iterable<Vertex> qResult = graph.getVertices("artist.name", artistName); // graph.command(new OCommandSQL(query)).execute();

				Vertex artistVertex = null;
				if (qResult.iterator().hasNext()){
					artistVertex = qResult.iterator().next();
					artistVertex.setProperty("date", date);
				}else{
					System.out.println(">>> WARNING: did not find artist: "+artistName);
				}

			}
			line = inputReader.readLine();
			
		}
		inputReader.close();
    }
	
 
}