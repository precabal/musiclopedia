import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class DB_Manager {
	
	OrientGraph graph;
	
	public static void main(String[] args) {

    	if (args.length < 2) {
      		System.err.println("Usage: DistributedParse <inputKeys> <edges>");
      		System.exit(1);
    	}
    	String keysFilePath = args[0];
    	String edgesFilePath = args[1];
    	
    	DB_Manager manager = new DB_Manager();
    	
    	manager.loadVerticesAndEdges(keysFilePath, edgesFilePath);
    	
		
    	
	}

	public void loadVerticesAndEdges(String keys, String edges) {
		
		OrientGraphFactory factory = new OrientGraphFactory("plocal:~/data/temp/graph/db");
		graph = factory.getTx();
		
		graph.createVertexType("url");
		graph.createVertexType("artist");
		
		try{
			Map<String,Vertex> vertexMap = loadVertices(keys);
			insertEdges(edges,vertexMap);
			
			graph.commit();
			
		} catch( Exception e ) {
			graph.rollback();

		} finally {
			graph.shutdown();
			factory.close();
		}
		
    }
	
	private Map<String,Vertex> loadVertices(String inputFile) throws IOException {
		
		Map<String,Vertex> vertexMap = new HashMap<String,Vertex>();
        
		
		BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));	
		String line = inputReader.readLine();
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
			
			/* consider using graph.getVertexByKey("name", artist) isntead */
			graph.addEdge(null,urlVertex, vertexMap.get(artist), "contains");
			
			line = inputReader.readLine();
		}
		inputReader.close();

    }
 
}