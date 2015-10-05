graph = new OrientGraph("plocal:/home/ubuntu/project/data/releases/orientdb-community-2.1.2/databases/finaldb");

//select all artists
println ">>> Executing script..."

def artistPipe = getAllArtists(graph);
artistList=[];artistPipe.fill(artistList);

//def artistCount = artistList.size();
//println "artist count: " + artistCount;

//TODO define this as a multiple of the average percentage of relation
def threshold = 0.0133;

(artistRelationships, sumPerKey) = calculateRelationships(artistList)

//iterate all pairs, and if it is larger than the threshold, create a vertex
artistRelationships.each{
	i -> 
	i.value.each{
		j ->
		if(i.key != trimId(j.key.id)){	
			def x_over_i = j.value.div(sumPerKey.get(i.key));
			def x_over_j = j.value.div(sumPerKey.get(trimId(j.key.id)));
			def percentage =  geometricMean(x_over_j,x_over_i);
			if (percentage>threshold){
				//println "arti "+ graph.v(i.key).name + " has "+percentage+" % of " + j.key.name + ".";
				
				date_i = graph.v(i.key).date;
				date_j = j.key.date;	
				//println date_i +" -> "+ date_j
				
				
				if ((date_i <= date_j) && (date_i != 0) && (date_j != 0)){
					graph.addEdge(graph.v(i.key),j.key,'influences');	
				
				}
			}
		}
	}
}
graph.stopTransaction(SUCCESS);

def geometricMean(x,y){
	return (x+y)/2;  //Math.min(x,y) //Math.sqrt(x*y)
}
//def averagePercentage = (sumOfAllSum-selfReferencesSum).div(artistCount*(artistCount-1));

GremlinGroovyPipeline getAllArtists(OrientGraph graph) {
	return graph.V.in.back(1);
}


def calculateRelationships(List artistList){
	def pairs =[:]
	def sumPerKey =[:]
	artistList.each{
	
		def rid = trimId(it.id);
		pairs.put(rid, getRelatedArtists(rid) );

       		// def selfReferences = pairs[rid].get(graph.v(rid));
       		// selfReferencesSum += selfReferences;
        
		sumPerKey.put(rid, pairs[rid].values().sum() );
        	//sumOfAllSum += sumOfAll;	
	}
	return [pairs, sumPerKey];
}
def trimId(id){
	return id.toString().substring(1);
}

LinkedHashMap getRelatedArtists(String artistId) {
	def artistName = graph.v(artistId).name;
	//println artistName + " related artists:";
	def map = [:]
	graph.v(artistId).in.out.groupCount(map).iterate();
	//map.sort{a,b -> b.value <=> a.value};
	return map;
}

int getSelfCount(String artistId) {
        def map = [:]
        graph.v(artistId).sideEffect{x=it}.in.out.filter{it == x}.name.groupCount(map).iterate();
        def nombre = graph.v(artistId).name;
	return map.get(nombre);
}
