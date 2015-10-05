import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class DateImporter {
	
	private OrientGraphNoTx graph;
	private OrientGraphFactory factory;
	
	public static void main(String[] args) {

    	if (args.length < 2) {
      		System.err.println("Usage: DB_Manager <inputKeys> <db_path>");
      		System.exit(1);
    	}
    	String keysFilePath = args[0];
    	String dbPath = args[1];
    	
    	DateImporter importer = new DateImporter();
    	
    	importer.loadDatabase(dbPath);
    	importer.loadVertices(keysFilePath);    	
		
	}
	public void loadDatabase(String databasePath){
		factory = new OrientGraphFactory(databasePath);
		graph = factory.getNoTx(); //.getTx();
		
	}


	public void loadVertices(String keys) {
		
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

				Iterable<Vertex> vertices = graph.getVertices("artist.name", artistName);

				Vertex artistVertex = null;
				if (vertices.iterator().hasNext()){
					artistVertex = vertices.iterator().next();
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