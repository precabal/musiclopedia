import java.io.BufferedReader;
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

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

public class DB_Manager {
	
	private OrientGraphNoTx graph;
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
		
			String sql = "CREATE PROPERTY artist.date integer";
			OCommandSQL createIndex = new OCommandSQL(sql);
			graph.command(createIndex).execute(new Object[0]);
		
			sql = "CREATE PROPERTY artist.name string";
			createIndex = new OCommandSQL(sql);
			graph.command(createIndex).execute(new Object[0]);
	
			sql = "create index artist.name on artist (name) unique";
			createIndex = new OCommandSQL(sql);
			graph.command(createIndex).execute(new Object[0]);
		}

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
    	
    	int directoryCount =0;
    	for(FileStatus item : items){
    		//System.out.println(item.getPath().toString());
            // ignoring files like _SUCCESS
            if(item.getPath().getName().startsWith("_")) {
              continue;
            }
    		
    		if(item.isDirectory()){
    			System.out.print(directoryCount++);
    			/* if file is a folder, call itself recursively */
    			 if(item.getPath().getName().startsWith("output")){
    				 insertEdges(item.getPath());
    			 }
    		}else{
    	    	/* if it's a file, get the lines within it */
    	    	List<String> lines = getLines(item.getPath().toString(), fileSystem);
    			
    	    	//int lineCount=0;
    	    	/*and add them as vertices to the db */
    	    	for(String line : lines){
    	    		//System.out.print(" "+lineCount++);
    	    		if(line.length()!=0)
    	    			addLineToDB(line);   
    	    	}
    	    	//System.out.print("\n");
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
    	
        stream.close();
        
    	return results;
    }
    
    private void addLineToDB(String line){
    	
    	int p = line.lastIndexOf(",");
    	
    	String url = line.substring(0,p);
    	String artist = line.substring(p+1);
    	
    	Vertex urlVertex = null;
    	
    	//String query = String.format("SELECT FROM INDEX:url.name WHERE key = '%s'", url);
    	
    	Iterable<Vertex> qResult =graph.getVertices("url.name", url); // graph.command(new OCommandSQL(query)).execute();
    	
    	if (qResult.iterator().hasNext()){
    		//System.out.println(url);
    		//System.out.println(qResult.iterator().next().toString());
    		urlVertex = qResult.iterator().next();
    		
    		//System.out.println(url+" "+urlVertex.toString());
    	}else{
    		urlVertex = graph.addVertex("class:url");
    		urlVertex.setProperty("name", url);
    	}

    	Vertex artistVertex = vertexMap.get(artist);
    	if(artistVertex!=null)
    		graph.addEdge(null,urlVertex, artistVertex, "contains");
    	else{
    		System.out.println("could not find artist "+artist+" described in line "+line);
    	}

    }
    

 
}