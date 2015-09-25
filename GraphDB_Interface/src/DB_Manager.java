import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class DB_Manager {
	
	OrientGraph graph;
	
	public static void main(String[] args) {

    	if (args.length < 3) {
      		System.err.println("Usage: DB_Manager <inputKeys> <edges> <db_path>");
      		System.exit(1);
    	}
    	String keysFilePath = args[0];
    	String edgesFilePath = args[1];
    	String dbPath = args[2]; //"plocal:/home/ubuntu/project/releases/orientdb-community-2.1.2/databases/db"
    	
    	DB_Manager manager = new DB_Manager();
    	
    	manager.loadVerticesAndEdges(keysFilePath, edgesFilePath, dbPath);
    	
		
    	
	}

	public void loadVerticesAndEdges(String keys, String edges, String databasePath) {
		
		OrientGraphFactory factory = new OrientGraphFactory(databasePath);
		graph = factory.getTx();
		
		if(graph.getVertexType("url")==null)
			graph.createVertexType("url");
		if(graph.getVertexType("artist")==null)
			graph.createVertexType("artist");
		
		try{
			Map<String,Vertex> vertexMap = loadVertices(keys);
			System.out.println("done vertex");
			insertEdges(edges,vertexMap);
			
			graph.commit();
			
		} catch( Exception e ) {
			System.out.println("caught an exeption");
			e.printStackTrace();
			graph.rollback();

		} finally {
			graph.shutdown();
			factory.close();
		}
		
    }
	
	private Map<String,Vertex> loadVertices(String inputFile) throws IOException {
		
		Map<String,Vertex> vertexMap = new HashMap<String,Vertex>();
        
		System.out.println("im vertex");
		BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));	
		String line = inputReader.readLine();
		System.out.println(line);
		while(line!=null){
			
			Vertex vertex = graph.addVertex("class:artist");
			vertex.setProperty("name", line);
			vertexMap.put(line, vertex);
			
			line = inputReader.readLine();
		}
		inputReader.close();
		
		return vertexMap;
		
    }
	
    private void insertEdges(String inputFile, Map<String, Vertex> vertexMap) throws IOException {
 
    	BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));	
		String line = inputReader.readLine();
		while(line!=null){
			String url = line.split(",")[0];
			
			
			Vertex urlVertex = graph.addVertex("class:url");
			urlVertex.setProperty("name", url);
			
			String artist = line.split(",")[1];
			System.out.println(artist);
			/* consider using graph.getVertexByKey("name", artist) isntead */
			graph.addEdge(null,urlVertex, vertexMap.get(artist), "contains");
			
			line = inputReader.readLine();
		}
		inputReader.close();

    }
 
}