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
SNAPSHOT_PREFIX = "olt_snapshot"

# helper functions
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

# process functions
def load_snapshot(dbconnstr):
    logging.debug("CALL load_snapshot ("+dbconnstr+")")
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    snapshot_names = db.collection_names(include_system_collections=False)
    snapshot_latest = None
    for n in snapshot_names:
        if n.startswith(SNAPSHOT_PREFIX):
            n_ts = int(n[len(SNAPSHOT_PREFIX)+1:])
            c_ts = 0
            if snapshot_latest != None:
                c_ts = int(snapshot_latest[len(SNAPSHOT_PREFIX)+1:])
            if c_ts < n_ts:
                snapshot_latest = n
    ret = None
    if snapshot_latest != None:
        ret = dict()
        snapshot_data = list(db[snapshot_latest].find())
        for e in snapshot_data:
            pfx = e['pfx']
            asn = e['asn']
            ttl = e['ttl']
            if pfx not in ret:
                ret[pfx] = dict()
            ret[pfx][asn] = (ttl[0],ttl[1])
    return ret

def remove_snapshot(ts, dbconnstr):
    logging.debug("CALL remove_snapshot ("+dbconnstr+")")
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    rs = SNAPSHOT_PREFIX + "_" + str(ts)
    try:
        db.drop_collection(rs)
    except Exception, e:
        logging.exception ("FAIL remove snapshot, with: %s" , e.message)
    else:
        logging.info("SUCCESS remove snapshot")

def store_snapshot(ts, lts, dbconnstr):
    logging.debug("CALL store_snapshot ("+dbconnstr+")")
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    snapshot_name = SNAPSHOT_PREFIX + "_" + str(ts)
    if (snapshot_name in db.collection_names()) or (len(lts.keys()) < 1):
        logging.warn("SKIP, snapshot exists!")
    else:
        bulk = db[snapshot_name].initialize_unordered_bulk_op()
        for pfx in lts:
            for asn in lts[pfx]:
                ttl = lts[pfx][asn]
                bulk.insert({ 'pfx': pfx, 'asn': asn, 'ts': ts, 'ttl': [ ttl[2], ttl[3] ] })
        try:
            bulk.execute({'w': 0})
        except Exception, e:
            logging.exception ("FAIL bulk operation, with: %s" , e.message)
        else:
            logging.info("SUCCESS store snapshot.")

def print_origins_lt(ts, lt):
    logging.debug("CALL print_origins_lt (%10d,%10d)" % (ts,len(lt)))
    for l in lt:
        print "[%45s,%10s,%10d,%10d,%9d]" % (l[0],l[1],l[2],l[3],int(l[3]-l[2]))
    logging.info("--- EOD ---")

def store_origins_lt(ts, lt, dbconnstr):
    logging.debug("CALL store_origins_lt ("+dbconnstr+")")
    # open db connection
    client = MongoClient(dbconnstr)
    db = client.get_default_database()
    bulk = db.origins_lt.initialize_unordered_bulk_op()
    do_bulk = False
    for l in lt:
        bulk.insert({ 'pfx': l[0], 'asn': l[1], 'ts': ts, 'ttl': [ l[2], l[3] ] })
        do_bulk = True
    # end for loop
    if do_bulk:
        logging.debug("EXEC bulk operation (# "+str(len(lt))+")")
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
    parser.add_argument('-k', '--keepsnapshots',
                        help='Keep all snapshots, works only with -s.',
                        action='store_true')
    parser.add_argument('-s', '--snapshot',
                        help='Enable snapshoting.',
                        action='store_true')
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
            if args['snapshot'] and (len(rib_origins.keys()) > 0):
                store_snapshot(rec.time, rib_origins, mongodbstr)
                if not args['keepsnapshots']:
                    remove_snapshot(rib_ts, mongodbstr)
                # end if keepsnapshots
            # end if snapshot
            rib_ts = rec.time
            logging.info("ts: "+str(rib_ts))
            if len(origins_lt) > 0:
                if mongodbstr:
                    store_origins_lt(rib_ts,origins_lt, mongodbstr)
                else:
                    print_origins_lt(rib_ts,origins_lt)
                #end if
                for l in origins_lt:
                    del rib_origins[l[0]][l[1]]
                #end for
                origins_lt = list()
            # end if
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
        store_origins_lt(rib_ts,origins_lt, mongodbstr)
    else:
        print_origins_lt(rib_ts,origins_lt)
#end def

if __name__ == "__main__":
    main()
