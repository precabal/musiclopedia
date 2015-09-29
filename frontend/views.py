from frontend import frontend
from flask import render_template
import pyorient

client = pyorient.OrientDB("ec2-54-210-182-168.compute-1.amazonaws.com",2424)
session_id = client.connect("root", "music")
client.db_open("graphdb", "admin", "admin" )

@frontend.route('/')
@frontend.route('/index')
def index():
   user = { 'nickname': 'Miguel' } # fake user
   return render_template("index.html", title = 'Home', user = user)


@frontend.route('/api/<artist_name>')
def get_connections(artist_name):
	print artist_name
	statement = "SELECT FROM E WHERE in.name=\'"+artist_name+"\'"
	#print statement
	response = client.query(statement)
	response_list = []
       
	for record in response:
		statement = "SELECT name FROM "+record._out.get()
		response = client.query(statement)
		response_list.append(response[0].oRecordData['name'])

	return render_template("index.html", title = 'Home', resultList=response_list)

def email():
 return render_template("base.html")