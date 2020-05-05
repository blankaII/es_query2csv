#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Version: v0.1.4
Usage: python es_query -h
Author: Glenn Chang
Purpose: query elasticsearch index by flexible template and config
Comments:
"""

import imports
from settings import *

import argparse
import ConfigParser
import csv
import json
import os
import pprint
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from elasticsearch import Elasticsearch
from jinja2 import Environment, FileSystemLoader

def search(cfg_file,number_of_doc,query_template,get_all_doc,output_file):

    config = ConfigParser.ConfigParser()
    config.read(os.path.join(cfg_file))
    es_server = []
    es_server.append(config.get('elasticsearch','servers'))
    es_timeout = int(config.get('elasticsearch', 'timeout'))
    es_index = []
    es_index.append(config.get('elasticsearch','index'))

    pp = pprint.PrettyPrinter(indent=4)

    print "elasticsearch servers:",es_server
    print "elasticsearch query timeout:", es_timeout
    print "search index:",es_index

    es = Elasticsearch(
        es_server,
        timeout=es_timeout
    )

    j2_env = Environment(loader=FileSystemLoader(os.path.dirname(query_template)),
                         trim_blocks=True)
    doc = str(j2_env.get_template(os.path.basename(query_template)).render())

    headers = json.loads(j2_env.get_template(os.path.basename(query_template)).render())["_source"]
    fetch_count = number_of_doc

    print "------------------------------"
    print "start to query..."
    res = es.search(
        index=es_index,
        scroll='1m',
        body=doc
    )

    if not 'error' in res:
        scroll_size = res['hits']['total']
        print "number of total match docs:", str(scroll_size)
        if scroll_size <=0:
            sys.exit()

        sid = res['_scroll_id']

        headers_line = ",".join(headers)
        print "start to write to output file..."
        with open(output_file,"w") as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',',
                                   quotechar='\"', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(headers)

        print "scrol size=",len(res['hits']['hits'])
        if get_all_doc or (number_of_doc >= scroll_size):
            while(scroll_size > 0):
                with open(output_file,"a") as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',',
                                           quotechar='\"', quoting=csv.QUOTE_MINIMAL)
                    for hit in res["hits"]["hits"]:
                        hit_list = []
                        for header in headers:
                            if not header in hit["_source"]:
                                if not "fields" in hit:
                                    hit_list.append("")
                                else:
                                    if not header in hit["fields"]:
                                        hit_list.append("")
                                    else:
                                        hit_list.append(hit["fields"][header])
                            else:
                                hit_list.append(hit["_source"][header])
                        spamwriter.writerow(hit_list)
                res = es.scroll(scroll_id=sid, scroll = '1m')
                sid = res['_scroll_id']
                scroll_size = len(res['hits']['hits'])
                sys.stdout.write('S')
                sys.stdout.flush()
            print "[query finished]"
        else:
            while(fetch_count > 0):
                with open(output_file,"a") as csvfile:
                    spamwriter = csv.writer(csvfile, delimiter=',',
                                           quotechar='\"', quoting=csv.QUOTE_MINIMAL)
                    for hit in res["hits"]["hits"]:
                        if fetch_count > 0:
                            fetch_count = fetch_count -1
                            hit_list = []
                            for header in headers:
                                if not header in hit["_source"]:
                                    if not "fields" in hit:
                                        hit_list.append("")
                                    else:
                                        if not header in hit["fields"]:
                                            hit_list.append("")
                                        else:
                                            hit_list.append(hit["fields"][header])
                                else:
                                    hit_list.append(hit["_source"][header])
                            spamwriter.writerow(hit_list)
                        else:
                            break
                if fetch_count > 0:
                    res = es.scroll(scroll_id=sid, scroll = '1m')
                    sid = res['_scroll_id']
                    scroll_size = len(res['hits']['hits'])
                    sys.stdout.write('S')
                    sys.stdout.flush()
            print "[query finished]"
    else:
        pp.pprint(res)



def main():
    parser = argparse.ArgumentParser(description='elasticsearch query tool, store result into csv format file')
    parser.add_argument('-v','--version', action="version",
                        version="%(prog)s Python Version v0.1.4")
    parser.add_argument('--get-all-doc',dest='get_all_doc',
                       action='store_true',
                       help='get all query record')
    parser.add_argument('--number-of-doc',dest='number_of_doc',type=int,
                       help='specify a number of doc you want to fetch')
    parser.add_argument('--cfg-file',dest='cfg_file',type=str,
                       help='specify configuration file')
    parser.add_argument('--template-file',dest='query_template',type=str,
                       help='specify query file')
    parser.add_argument('--output-file',dest='output_file',type=str,
                       help='specify output file')
    args = parser.parse_args()

    print "=============================="
    if args.cfg_file is None:
        args.cfg_file = os.path.join(APP_CONFIG,'default.ini')
    args.cfg_file = os.path.abspath(args.cfg_file)
    print "using configuration file:",args.cfg_file

    if args.query_template is None:
        args.query_template = os.path.join(APP_TEMPLATE,'default.j2')
    args.query_template = os.path.abspath(args.query_template)
    print "using query template:", args.query_template

    if args.output_file is None:
        args.output_file = os.path.join(APP_EXPORT,'output.csv')
    args.output_file = os.path.abspath(args.output_file)
    print "using output file:",args.output_file

    if args.number_of_doc is None:
        print "number of doc is not specified, set to 10 as default"
        args.number_of_doc = 10
    print "using number of doc:", args.number_of_doc

    search(
        cfg_file=args.cfg_file,
        query_template=args.query_template,
        number_of_doc=args.number_of_doc,
        get_all_doc=args.get_all_doc,
        output_file=args.output_file
    )

if __name__ == "__main__":
    main()
