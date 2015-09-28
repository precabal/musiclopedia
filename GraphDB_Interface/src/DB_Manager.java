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
			//System.out.println("done vertex");
			insertEdges(edges,vertexMap);
			
			graph.commit();
			
		} catch( Exception e ) {
			e.printStackTrace();
			graph.rollback();

		} finally {
			graph.shutdown();
			factory.close();
		}
		
    }
	
	
	private Map<String,Vertex> loadVertices(String inputFile) throws IOException {
		
		Map<String,Vertex> artistVertexMap = new HashMap<String,Vertex>();
        
		//System.out.println("im vertex");
		
		BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));	
		
		String line = inputReader.readLine();
		//System.out.println(line);
		while(line!=null){
			
			Vertex vertex = graph.addVertex("class:artist");
			vertex.setProperty("name", line);
			artistVertexMap.put(line, vertex);
			
			line = inputReader.readLine();
			
		}
		inputReader.close();
		
		return artistVertexMap;
		
    }
	
    private void insertEdges(String inputDirectory, Map<String, Vertex> vertexMap) {
 

    	Map<String,Vertex> urlVertexMap = new HashMap<String,Vertex>();
    	
		File[] files = new File(inputDirectory).listFiles();
		
	    for (File file : files) {
	        if (!file.isDirectory()) {
	        	String path = inputDirectory.concat("/").concat(file.getName());
	    
	        	Path location = new Path(path);
	        	
	        	List<String> results;
				try {
					results = readLines(location, new Configuration());

					for(String linex : results){

						String url = linex.split(",")[0];

						Vertex urlVertex = urlVertexMap.get(url);
						if(urlVertex==null){
							urlVertex = graph.addVertex("class:url");
							urlVertex.setProperty("name", url);
							urlVertexMap.put(url, urlVertex);
						}

						String artist = linex.split(",")[1];

						Vertex artistVertex = vertexMap.get(artist);
						if(artistVertex!=null)
							graph.addEdge(null,urlVertex, artistVertex, "contains");


					}
				} catch (Exception e) {
					System.out.println("Can't load file : " + path);
					e.printStackTrace();
				}
	    		
	        }
	    }


    }
    public List<String> readLines(Path location, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);
        if (items == null) return new ArrayList<String>();
        List<String> results = new ArrayList<String>();
        for(FileStatus item: items) {

          // ignoring files like _SUCCESS
          if(item.getPath().getName().startsWith("_")) {
            continue;
          }

          CompressionCodec codec = factory.getCodec(item.getPath());
          InputStream stream = null;

          // check if we have a compression codec we need to use
          if (codec != null) {
            stream = codec.createInputStream(fileSystem.open(item.getPath()));
          }
          else {
            stream = fileSystem.open(item.getPath());
          }

          StringWriter writer = new StringWriter();
          IOUtils.copy(stream, writer, "UTF-8");
          String raw = writer.toString();
          String[] resulting = raw.split("\n");
          for(String str: raw.split("\n")) {
            results.add(str);
          }
        }
        return results;
      }
 
}