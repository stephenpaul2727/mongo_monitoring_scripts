#!/usr/bin/python
####################################################################
##
#   Shard Distributions on collections of  Mongo DB Servers
##
####################################################################

from __future__ import division
import socket
import json
from bson.json_util import loads
from bson.json_util import dumps
from datetime import datetime, timedelta
import time
import pymongo
import os
import re
import logging
from logging import handlers

# GLOBAL VARIABLES
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename="/tmp/sharddist.log", level=logging.INFO)
logger = logging.getLogger(__name__)

def getHostName():
    hostName = socket.gethostname()
    hostNamePieces = hostName.split(".")
    hostName = hostNamePieces[0]
    return hostName

def getCurrentTimeStamp():
    return str(int(time.mktime(datetime.utcnow().replace(minute=0, second=0, microsecond=0).utctimetuple())))

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

    logger.info("Pushing Shard Distribution stats to Graphite started")

    # Defining the mongodb servers we need to monitor
    mongod = "MONGOSHOSTNAME"
    mongodb_servers_port = 27017
    mongodb_servers_db = "DBNAME"

    # Get the current running operations on the given mongodb_servers
    getShardDistribution(mongod, mongodb_servers_port, mongodb_servers_db)
    logger.info("Pushing Finished")


def getShardDistribution(mongod, mongodport, mongoddb):
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

    finalStr = ""
    db = connect(mongod, mongodport, mongoddb)
    collections = ["collection1", "collection2", "collection3"]

    for collection in collections:
        try:
            colstats = db.command("collstats", collection)
        except Exception as e:
            logger.error("Exception occured: %s",e)
            continue

        if colstats["sharded"] == False:
            logger.info("%s is not sharded. Please check the collections list before you run this program...!",collection)
            continue
        colsize =  colstats["size"]
        for shardname,shardvalue in colstats["shards"].iteritems():
            shardsize = shardvalue["size"]
            sharddatapercent = int(round(((shardsize/colsize)*100),0))
            modifiedcolname = collection
            logger.info("%s collection: %s shard has %d %% of total data",modifiedcolname, shardname, sharddatapercent)
            #print(hostname+"."+"scripts.sharddistribution."+modifiedcolname+"."+shardname+" "+str(sharddatapercent)+" "+currentTs+"\n")
            sock.sendall(hostname+"."+"scripts.sharddistribution."+modifiedcolname+"."+shardname+" "+str(sharddatapercent)+" "+currentTs+"\n")

if __name__=="__main__":
    main()
