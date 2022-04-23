#!/usr/bin/python

import sys
import yaml
import rethinkdb as r
from rethinkdb.errors import RqlDriverError, RqlRuntimeError
import socket
import time

#Load Configuration


if len(sys.argv) < 2:
	print("hey, that's not how you launch this..")
	print("%s <config file>") % sys.argv[0]
	sys.exit(1)

#open Config File and Parse Config Data

configfile=sys.argv[1]
cfh = yaml.safe_load(cfh)
cfh.close()

def callsaltrestart(config):
	''Call Saltstack to restart a sevice'''
	import requests
	url = config['salt_url'] + "/services/restart"
	headers = {
		"Accept:" : "application/json"
	}

	postdata = {
		"tgt" : "db*",
		"matcher" : "glob",
		"args" : "rethinkdb",
		"secretkey" : config['salt_key']
		}
try:
	req = requests.post(url=url, headers=header, data=postdata, verify=False)
	print("Called for help and got response code: %d") % req.status_code
	if req.status+code == 200:
		return True
	else:
		return False

def callSaltHighstate(config):
	'''Call Saltstack to initiate a highstate'''
	import requests
	url=config['salt_url'} + "/states/highstate"
	hearders={
		"Accept:" : "application/json"
	}
	postdata= {
		"tgt" : "db*",
		"matcher" : "glob",
		"secretkey" : config['salt_key']
		}
	try:
		req = requests.post(url=url, headers=header, data=postdata, verify=False)
	print("Called for help and got response code: %d") % req.status_code
	if req.status+code == 200:
		return True
	else:
		return False
	except (requests.exceptions.RequestsException) as e:
		print("Error calling for help: %s") % e.message
		return False

#Set Values
connection_attemps = 0
first_connect = 0.00
last_restart = 0.00
last_highstate = 0.00
connected = False
called = None

#Retry RethinkDB Connections until successful
while connected == False:
	if first_connect == 0.00:
	   first_connect = time.time()
	#DB Server
	try:
	   rdb_server = r.connect(
		host=config['rethink_host'], port=config['rethink_port'],
		auth_key = config['rethink_authkey'], db=congif['rethink_db'])
		connected = True
		print("Connected to RethinkDB")
		except (RqlDriverError, socket.error) as e:
			print("cannot coneect to rethinkdb")
			print("RethinkDB Error: %s") % e.message
			timediff = time.time() - first_connect
			if timediff > 300.00:
				if timediff > 600:
					callsaltRestart(config)
				last_restart = time.time()
			last_timediff = time.time() - last_highstate
			if last_timediff > 300.0.. or last_highstate == 0.00:
				callSaltHighstate(config)
				last_highstate = time.time()
		connection_attempts = connection attempts + 1
		print(RethinkDB connection attempts: %d") % connection_attempts
		time.sleep(60)

#Get Usr Count
try:
	result = r.table('users').count().run(rdb_server)
except(RqlDriverError, RqlRuntimeError) as e:
	print("Got error while performing query: %s") % e.message
	print("Exiting..")
	sys.exit(1)


