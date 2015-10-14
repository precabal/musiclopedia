import pyorient

class DBManager:

	def __init__(self, credentials):
		self.server1 = credentials.serverD2name
		self.server2 = credentials.serverD3name
		self.userName = credentials.userName
		self.password = credentials.password
		self.port = credentials.port
		self.runningServer = None
		self.client = None

	def connectToDBServer(self):
		
		targetServer = self.getAvailableServer()
		
		try:
			print ">>> attempting to connect to:         " + targetServer
			self.runningServer = targetServer
			self.client = pyorient.OrientDB(targetServer, self.port)
			session_id = self.client.connect(self.userName, self.password)	

		except pyorient.exceptions.PyOrientConnectionException:

			self.runningServer = self.getAvailableServer()
			print ">>> attempting instead to connect to: " + self.runningServer
			self.client = pyorient.OrientDB(self.runningServer, self.port)
			self.session_id = self.client.connect(self.userName, self.password)

		print ">>> connected to server:              " + self.runningServer	

	def getAvailableServer(self):
		availableServer = None
		if self.runningServer == self.server1:
			availableServer = self.server2
		else:
			availableServer = self.server1
		return availableServer