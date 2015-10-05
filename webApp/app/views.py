from app import app
from flask import render_template, request
import pyorient


client = pyorient.OrientDB("52.1.220.20",2424)
session_id = client.connect("root", "music")
client.db_open("finaldb", "admin", "admin" )
			

@app.route('/')
@app.route('/index')
@app.route('/email')
def email():
	return render_template("interface.html")

@app.route('/slides')
def slides():
	return render_template("slides.html")

@app.route('/tree_data.csv')
def send_js():
    return app.send_static_file('data/tree_data.csv')


def getTree(depth, artist, parent, artistDate):
	print artist
	nodeInformation = [[artist,parent,artist,artistDate]];

	if depth==0:
		return nodeInformation


	statement = "select name, date from (select expand(in('influences')) from artist where name =\'"+artist+"\')"
	children = client.query(statement)	
	
	for child in children:
		if child.name != parent:
			nodeInformation.extend(getTree(depth-1,child.name, artist, child.date))

	return nodeInformation	


@app.route('/', methods=['POST'])
@app.route('/index', methods=['POST'])
@app.route("/email", methods=['POST'])
def email_post():
	depthLevel = 2;	
	artist_name = request.form["emailid"].title()
 	treeInformation = getTree(depthLevel, artist_name.encode('utf-8'),-1,0);
 	print treeInformation
	return render_template("emailop.html", title = 'Home', artist=artist_name, treeData=treeInformation)
