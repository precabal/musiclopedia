from app import app
from flask import render_template, request
import pyorient

client = pyorient.OrientDB("52.1.220.20",2424)
session_id = client.connect("root", "music")
client.db_open("graphdb2", "admin", "admin" )

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

@app.route('/', methods=['POST'])
@app.route('/index', methods=['POST'])
@app.route("/email", methods=['POST'])
def email_post():
	artist_name = request.form["emailid"].title() #.capitalize() 
 	
	statement = "SELECT FROM E WHERE in.name=\'"+artist_name+"\'"
	# statement = "g.v('12:967').sideEffect{x=it}.in.out.filter{it != x}.name.groupCount()"
    #response = session.execute(stmt, parameters=[emailid, date])

	response = client.query(statement)
	print response
	response_list = []
	for record in response:
		statement = "SELECT name FROM "+record._out.get()
		response = client.query(statement)
		url = response[0].oRecordData['name']
		prefix = "http://"
		if url[0] == "/":
			prefix = "https:/"
		url = prefix+url
		response_list.append(url)
	#return render_template("emailop.html", output=jsonresponse)	
	return render_template("emailop.html", title = 'Home', resultList=response_list, artist=artist_name)