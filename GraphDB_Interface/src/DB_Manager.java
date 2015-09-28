import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class DB_Manager {
	
	private OrientGraph graph;
	private OrientGraphFactory factory;
	private Map<String,Vertex> vertexMap;
	
	public static void main(String[] args) {

    	if (args.length < 3) {
      		System.err.println("Usage: DB_Manager <inputKeys> <edges> <db_path>");
      		System.exit(1);
    	}
    	String keysFilePath = args[0];
    	String edgesFilePath = args[1];
    	String dbPath = args[2];
    	
    	DB_Manager manager = new DB_Manager();
    	
    	manager.loadDatabase(dbPath);
    	manager.createClasses();
    	manager.loadVerticesAndEdges(keysFilePath, edgesFilePath);
    	
		
	}
	public void loadDatabase(String databasePath){
		factory = new OrientGraphFactory(databasePath);
		graph = factory.getTx();
	}
	public void createClasses(){
		if(graph.getVertexType("url")==null)
			graph.createVertexType("url");
		if(graph.getVertexType("artist")==null)
			graph.createVertexType("artist");
		
		
	}

	public void loadVerticesAndEdges(String keys, String edges) {
		
		try{
			vertexMap = insertVertices(keys);
			insertEdges(new Path(edges));
			graph.commit();
			
		} catch( Exception e ) {
			e.printStackTrace();
			graph.rollback();

		} finally {
			graph.shutdown();
			factory.close();
		}
		
    }
	
	
	private Map<String,Vertex> insertVertices(String inputFile) throws IOException {
		
		Map<String,Vertex> artistVertexMap = new HashMap<String,Vertex>();
		
		BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));	
		
		String line = inputReader.readLine();
		
		while(line!=null){
			
			Vertex vertex = graph.addVertex("class:artist");
			vertex.setProperty("name", line);
			artistVertexMap.put(line, vertex);
			
			line = inputReader.readLine();
			
		}
		inputReader.close();
		
		return artistVertexMap;
		
    }
	
    private void insertEdges(Path inputDirectory) throws IllegalArgumentException, IOException{
    	
    	FileSystem fileSystem = FileSystem.get(inputDirectory.toUri(), new Configuration());
    	/* get all files and folders */
    	FileStatus[] items = fileSystem.listStatus(inputDirectory);
    	
    	for(FileStatus item : items){
    		
            // ignoring files like _SUCCESS
            if(item.getPath().getName().startsWith("_")) {
              continue;
            }
    		
    		if(item.isDirectory()){
    			/* if file is a folder, call itself recursively */
    			insertEdges(item.getPath());
    		}else{
    	    	/* if it's a file, get the lines within it */
    	    	List<String> lines = getLines("path", fileSystem);
    			
    	    	/*and add them as vertices to the db */
    	    	for(String line : lines)
    	    		addLineToDB(line);    	    	
    		}
    	}

    }
    private List<String> getLines(String HDFSinputFilePath, FileSystem referenceFileSystem) throws IllegalArgumentException, IOException {
    	   
    	List<String> results = new ArrayList<String>();
    	
    	InputStream stream  = null;
    	
		stream = referenceFileSystem.open(new Path(HDFSinputFilePath));
		
    	StringWriter writer = new StringWriter();
        
		IOUtils.copy(stream, writer, "UTF-8");
		
        String raw = writer.toString();
        
        for(String str: raw.split("\n")) {
            results.add(str);
        }
    	
    	return results;
    }
    
    private void addLineToDB(String line){

    	String url = line.split(",")[0];
    	String artist = line.split(",")[1];
    	
    	Vertex urlVertex = null;
    	
    	String query = "SELECT * FROM  " + "url" + " WHERE " + "name" + " = ?";
    	List<ODocument> qResult = graph.command(new OCommandSQL(query)).execute(url);
    	
    	if (qResult.isEmpty()){
    		urlVertex = graph.addVertex("class:url");
    		urlVertex.setProperty("name", url);
    	}else{
    		urlVertex = graph.getVertex(qResult);
    	}

    	Vertex artistVertex = vertexMap.get(artist);
    	if(artistVertex!=null)
    		graph.addEdge(null,urlVertex, artistVertex, "contains");
    	else{
    		System.out.println("could not find artist "+artist);
    	}

    }
    

 
}