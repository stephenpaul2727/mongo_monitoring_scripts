#!/usr/bin/python
####################################################################
##
#   Long Active Operations on Mongo DB Servers
##
####################################################################

import socket
import json
from bson.json_util import loads
from bson.json_util import dumps
from datetime import datetime, timedelta
import time
import pymongo
import logging
from logging import handlers

# GLOBAL VARIABLES
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename="/var/log/stephen/currops.log", level=logging.DEBUG)
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
    logger.info("Pushing Current MongoDB Operations to Graphite started")

    # Setting up files for writing alert ops
    f_gt30 = open("/var/log/stephen/opsgt30","a")
    f_gt60 = open("/var/log/stephen/opsgt60","a")

    # Defining the mongodb servers we need to monitor
    mongodb_servers = ["db-01a", "db-01b", "db-02a", "db-02b"]
    mongodb_servers_port = 27018
    mongodb_servers_db = "admin"

    # Get the current running operations on the given mongodb_servers
    getCurrentOps(mongodb_servers, mongodb_servers_port, mongodb_servers_db, f_gt30, f_gt60)

    # Closing the files
    f_gt30.close()
    f_gt60.close()

    logger.info("Pushing Finished")


def getCurrentOps(mongoservers, mongoserverport, mongoserverdb, f30, f60):
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

    f60.write("\n----------------------------------- "+datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")+" ------------------------------------\n")
    f30.write("\n----------------------------------- "+datetime.utcnow().strftime("%m/%d/%Y %H:%M:%S")+" ------------------------------------\n")

    for server in mongoservers:
        finalStr = ""
        gt60 = 0
        gt30 = 0
        state = "secondary"
        db = connect(server, mongoserverport, mongoserverdb)
        if db.command('ismaster')['ismaster'] == True:
            state = "primary"

        currentops = db.current_op()
        inprogressops = currentops["inprog"]
        # Excluding local.oplog.rs as this is generating generic longrunning ops which are not affecting the db. Further investigation required
        for op in inprogressops:
            try:
                if op["secs_running"]>60 and op["ns"]!="local.oplog.rs":
                    gt60 += 1
                    f60.write(dumps(op,indent=4))
                    f60.write("\n")
                if op["secs_running"]>30 and op["ns"]!="local.oplog.rs":
                    gt30 += 1
                    f30.write(dumps(op,indent=4))
                    f30.write("\n")
            except Exception as e:
                logger.info("Exception %s",e)

        #print(hostname+".scripts.longopcount."+server+"."+state+".gt30 "+str(gt30)+" "+currentTs+"\n")
        sock.sendall(hostname+".scripts.longopcount."+server+"."+state+".gt60 "+str(gt60)+" "+currentTs+"\n")
        sock.sendall(hostname+".scripts.longopcount."+server+"."+state+".gt30 "+str(gt30)+" "+currentTs+"\n")


if __name__=="__main__":
    main()
