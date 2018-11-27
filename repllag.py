#!/usr/bin/python
####################################################################
##
#   Get Replication Lag Information for all the Secondary Instances
##
####################################################################

import socket
import json
from bson.json_util import loads
from bson.json_util import dumps
from datetime import datetime, timedelta, date
import time
import re
import pymongo
import logging
from logging import handlers

# GLOBAL VARIABLES
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename="/var/log/stephen/replinfo.log", level=logging.DEBUG)
logger = logging.getLogger(__name__)

def getHostName():
    hostName = socket.gethostname()
    hostNamePieces = hostName.split(".")
    hostName = hostNamePieces[0]
    return hostName

def getCurrentTimeStamp():
    return str(int(time.mktime(datetime.utcnow().replace(second=0, microsecond=0).utctimetuple())))

def connect(server, port, database):
    '''connect to mongo database and authenticate'''
    mongodb_uri = "mongodb://%s:%s/" % (server, port)
    logger.info("connecting to %s ......", mongodb_uri)
    try:
        logger.info("connection to mongodb successful...!")
        client = pymongo.MongoClient(mongodb_uri, read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED)
        return client[database]
    except (pymongo.errors.ConnectionFailure, pymongo.errors.ConfigurationError) as error:
        logger.error("could not connect to %s: %s", server, error)
        sys.exit(1)

def main():
    # Set up Logging to console and syslog
    logger.info("Pushing Replication Lag info to the Graphite Instance")

    # Defining the mongodb shards (any replica set of each shard is fine)
    mongodb_servers = ["db-01a", "db-02a"]
    mongodb_servers_port = 27018
    mongodb_servers_db = "admin"

    # Get the current running operations on the given mongodb_servers
    getReplicationLag(mongodb_servers, mongodb_servers_port, mongodb_servers_db)

    logger.info("Pushing Finished")


def getReplicationLag(mongoshards, mongoserverport, mongoserverdb):
    # Get the Hostname of the machine
    hostname = getHostName()

    # Getting current timestamp
    currentTs = getCurrentTimeStamp()

    # Socket connection to reports carbon"
    sock = socket.socket()

    # Connecting to Carbon on 2003
    try:
        logger.info("connecting to %s Graphite Carbon......",hostname)
        sock.connect(("127.0.0.1",2003))
        logger.info("connection to graphite successful")
    except socket.error as se:
        logger.error("connection to Graphite Failed..!")
        sys.exit(1)

    for shard in mongoshards:
        db = connect(shard,mongoserverport,mongoserverdb)
        replsetstatus = db.command("replSetGetStatus")
        opstartdate = None

        if len(replsetstatus["members"]) == 1:
            logger.info("Repl Set has just 1 member. Can't find Replication info in this scenario.")
            sys.exit(1)

        for member in replsetstatus["members"]:
            if member["stateStr"] == "PRIMARY":
                opstartdate = member["optimeDate"]

        for member in replsetstatus["members"]:
            if "jumphost" in member["name"]:
                continue
            elapsedtime = member["optimeDate"]-opstartdate
            state = member["stateStr"]
            servername = member["name"].rstrip(":27018")
            sock.sendall(hostname+".scripts.repllag."+servername+" "+str(int(abs(elapsedtime.total_seconds())))+" "+currentTs+"\n")
            #print(hostname+".scripts.repllag."+servername+" "+str(int(abs(elapsedtime.total_seconds())))+" "+currentTs+"\n")

if __name__=="__main__":
    main()

