from app import app
from flask import render_template, request
from DBManager import DBManager
from Credentials import Credentials
import pyorient


dbName = "finaldb"
dbUser = "admin"
dbPassword = "admin"

dbManager = DBManager(Credentials())
dbManager.connectToDBServer()
dbManager.client.db_open(dbName, dbUser, dbPassword)

@app.route('/')
@app.route('/index')
def email():
	return render_template("interface.html")

@app.route('/', methods=['POST'])
def email_post():
	depthLevel = 3;	
	artist_name = request.form["artistName"].title()
	queryType = "in" if (request.form["queryTypeSelector"] == "Influencers") else "out"
 	treeInformation = getTree(depthLevel, artist_name.encode('utf-8'), -1, '', 0, queryType);
 	
	return render_template("results.html", title = 'Home', artist=artist_name, treeData=treeInformation)

@app.route('/slides')
def slides():
	return render_template("slides.html")

def getTree(depth, artistName, parentID, parentName, birthDate, direction):
	
	nodeInformation = [[str(depth+hash(artistName)+hash(parentID)), parentID, artistName.decode('utf-8').encode('latin-1', 'replace'), birthDate]];
	
	if depth==0:
		return nodeInformation

	statement = "select name, date from (select expand("+direction+"('influences')) from artist where name =\'"+artistName+"\')"
	
	try:
		children = dbManager.client.query(statement)	
	except (pyorient.exceptions.PyOrientConnectionException, pyorient.exceptions.PyOrientDatabaseException):
		dbManager.connectToDBServer()
		dbManager.client.db_open(dbName, dbUser, dbPassword)
		children = dbManager.client.query(statement)

	for child in children:
		if child.name != parentName:
			nodeInformation.extend(getTree(depth-1, child.name, str(depth+hash(artistName)+hash(parentID)), artistName, child.date, direction))

	return nodeInformation	


