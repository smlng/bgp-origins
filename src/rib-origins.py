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
from _pybgpstream import BGPStream, BGPRecord, BGPElem

RIB_TS_INTERVAL = 120

# helper functions
def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

# process functions
def process_rib_origins(ts, origins):
    for p in origins:
        print p + " : " + ','.join(o for o in origins[p])
    print "timestamp: " + str(ts) + ", #prefixes: " + str(len(origins.keys()))
    print

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
    parser.add_argument('-l', '--loglevel',
                        help='Set loglevel [DEBUG,INFO,WARNING,ERROR,CRITICAL].',
                        type=str, default='WARNING')

    args = vars(parser.parse_args())

    numeric_level = getattr(logging, args['loglevel'].upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s : %(levelname)s : %(message)s')

    ts_begin = int((args['begin'] - datetime(1970, 1, 1)).total_seconds())
    ts_until = int((args['until'] - datetime(1970, 1, 1)).total_seconds())

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
    while(stream.get_next_record(rec)):
        if rec.status == 'valid':
            elem = rec.get_next_elem()
        if rec.time > (rib_ts + RIB_TS_INTERVAL):
            rib_ts = rec.time
            process_rib_origins(rib_ts, rib_origins)
            rib_origins = dict()
        while(elem):
            prefix = elem.fields['prefix']
            aspath = elem.fields['as-path'].split()
            for a in aspath:
                if '{' in a:
                    aspath.remove(a)
            origin = aspath[-1]
            if prefix not in rib_origins:
                rib_origins[prefix] = list()
            if origin not in rib_origins[prefix]:
                rib_origins[prefix].append(origin)
            elem = rec.get_next_elem()

if __name__ == "__main__":
    main()
