from app import app
from flask import render_template, request
import pyorient

class DBManager:

	def __init__(self):
		self.serverD2name = "ec2-54-173-195-130.compute-1.amazonaws.com"
		self.serverD3name = "ec2-54-173-189-239.compute-1.amazonaws.com"
		self.port = 2424
		self.runningServer = None
		self.client = None

	def connectToDBServer(self):
		
		targetServer = self.getAvailableServer()
		
		try:
			print ">>> attempting to connect to:         "+targetServer
			self.runningServer = targetServer
			self.client = pyorient.OrientDB(targetServer, self.port)
			
			session_id = self.client.connect("root", "music")	

		except pyorient.exceptions.PyOrientConnectionException:

			self.runningServer = self.getAvailableServer()
			print ">>> attempting instead to connect to: "+self.runningServer
			self.client = pyorient.OrientDB(self.runningServer, self.port)
			self.session_id = self.client.connect("root", "music")

		print ">>> connected to server:              " + self.runningServer	

	def getAvailableServer(self):
		availableServer = None
		if self.runningServer == self.serverD2name:
			availableServer = self.serverD3name
		else:
			availableServer = self.serverD2name
		return availableServer


dbManager = DBManager()
dbManager.connectToDBServer()
dbManager.client.db_open("finaldb", "admin", "admin")

@app.route('/')
@app.route('/index')
def email():
	return render_template("interface.html")

@app.route('/', methods=['POST'])
def email_post():
	depthLevel = 2;	
	artist_name = request.form["artistName"].title()
	queryType = "in" if (request.form["queryTypeSelector"] == "Influencers") else "out"
 	treeInformation = getTree(depthLevel, artist_name.encode('utf-8'), -1, 0, queryType);
	return render_template("results.html", title = 'Home', artist=artist_name, treeData=treeInformation)

@app.route('/slides')
def slides():
	return render_template("slides.html")

@app.route('/tree_data.csv')
def send_js():
    return app.send_static_file('data/tree_data.csv')


def getTree(depth, artist, parent, artistDate, direction):
	#print artist
	nodeInformation = [[artist,parent,artist,artistDate]];

	if depth==0:
		return nodeInformation

	statement = "select name, date from (select expand("+direction+"('influences')) from artist where name =\'"+artist+"\')"
	
	try:
		children = dbManager.client.query(statement)	
	except (pyorient.exceptions.PyOrientConnectionException, pyorient.exceptions.PyOrientDatabaseException):
		dbManager.connectToDBServer()
		dbManager.client.db_open("finaldb", "admin", "admin")
		children = dbManager.client.query(statement)

	for child in children:
		if child.name != parent:
			nodeInformation.extend(getTree(depth-1,child.name, artist, child.date,direction))

	return nodeInformation	


