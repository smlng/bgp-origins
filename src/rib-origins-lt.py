#!/usr/bin/env python
import argparse
import calendar
import json
import logging
import os
import re
import string
import sys

import multiprocessing as mp

from datetime import datetime
from subprocess import PIPE, Popen

from pymongo import MongoClient, DESCENDING, ASCENDING
from _pybgpstream import BGPStream, BGPRecord, BGPElem

# required, 'cause output of RIB takes some time, too -> not done in a second
RIB_TS_THRESHOLD = 300

# helper functions
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

# process functions
def print_origins_lt(origins_lt):
    logging.debug("CALL print_origins_lt")
    logging.info("#datasets: " + str(len(origins_lt)))
    logging.info("----------------------------------")
    for l in origins_lt:
        print "[%45s,%10s,%10d,%10d,%9d]" % (l[0],l[1],l[2],l[3],int(l[3]-l[2]))
    logging.info("----------------------------------")

def store_origins_lt(origins_lt, dbconnstr):
    logging.debug("CALL store_origins_lt ("+dbconnstr+")")
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    bulk = db.origins_lt.initialize_unordered_bulk_op()
    do_bulk = False
    for l in origins_lt:
        bulk.insert({ 'pfx': l[0], "asn": l[1], 'ttl': [ l[2], l[3] ] })
        do_bulk = True
    # end for loop
    if do_bulk:
        logging.debug("EXEC bulk operation (#"+str(len(origins_lt))+")")
        try:
            bulk.execute({'w': 0})
        except Exception, e:
            logging.exception ("FAIL bulk operation, with: %s" , e.message)
    else:
        logging.warn("PASS bulk operation, no data")

def main():
    parser = argparse.ArgumentParser(description='', epilog='')
    parser.add_argument('-b', '--begin',
                        help='Begin date (inclusive), format: yyyy-mm-dd HH:MM',
                        type=valid_date, required=True)
    parser.add_argument('-u', '--until',
                        help='Until date (exclusive), format: yyyy-mm-dd HH:MM',
                        type=valid_date, required=True)
    parser.add_argument('-c', '--collector',
                        help='Route collector from RIPE RIS or Route-Views project.',
                        type=str, required=True)
    parser.add_argument('-m', '--mongodb',
                        help='MongoDB connection parameters.',
                        type=str, default=None)
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='WARNING')

    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='#> %(asctime)s : %(levelname)s : %(message)s')

    ts_begin = int((args['begin'] - datetime(1970, 1, 1)).total_seconds())
    ts_until = int((args['until'] - datetime(1970, 1, 1)).total_seconds())

    mongodbstr = None
    if args['mongodb']:
        mongodbstr = args['mongodb'].strip()
    # BEGIN
    logging.info("START")

    # Create bgpstream
    stream = BGPStream()
    rec = BGPRecord()
    # set filtering
    stream.add_filter('collector',args['collector'])
    stream.add_filter('record-type','ribs')
    stream.add_interval_filter(ts_begin,ts_until)

    # Start the stream
    stream.start()

    rib_ts = 0
    rib_origins = dict()
    origins_lt = list()
    while(stream.get_next_record(rec)):
        if rec.status == 'valid':
            elem = rec.get_next_elem()
        else:
            logging.warn("stream record invalid, skipping ...")
            continue
        #end if
        if rec.time > (rib_ts + RIB_TS_THRESHOLD):
            for p in rib_origins:
                for o in rib_origins[p]:
                    if rib_origins[p][o][1] < (rib_ts - RIB_TS_THRESHOLD):
                        origins_lt.append( (p,o,rib_origins[p][o][0],rib_origins[p][o][1]) )
                    #end if
                #end for
            #end for
            if len(origins_lt) > 0:
                if mongodbstr:
                    store_origins_lt(origins_lt, mongodbstr)
                else:
                    print_origins_lt(origins_lt)
                #end if
                for l in origins_lt:
                    del rib_origins[l[0]][l[1]]
                #end for
                origins_lt = list()
            # end if
            rib_ts = rec.time
            logging.info("ts: "+str(rib_ts))
        #end if
        while(elem):
            prefix = elem.fields['prefix']
            aspath = elem.fields['as-path'].split()
            for a in aspath: # remove AS-SETs
                if '{' in a:
                    aspath.remove(a)
                #end if
            #end for
            origin = aspath[-1]
            if prefix not in rib_origins:
                rib_origins[prefix] = dict()
            #end if
            if origin not in rib_origins[prefix]:
                rib_origins[prefix][origin] = (rib_ts,rib_ts)
            else:
                rib_origins[prefix][origin] = (rib_origins[prefix][origin][0],rib_ts)
            #end if
            elem = rec.get_next_elem()
        #end while
    #end while
    for p in rib_origins:
        for o in rib_origins[p]:
            origins_lt.append( (p,o,rib_origins[p][o][0],rib_ts) )
    if mongodbstr:
        store_origins_lt(origins_lt, mongodbstr)
    else:
        print_origins_lt(origins_lt)
#end def

if __name__ == "__main__":
    main()
