#!/usr/bin/python

import sys
import getopt
import random
from datetime import datetime
from elasticsearch import Elasticsearch

es = None


def us(td):
  return (td.days * 24 * 60 * 60 + td.seconds) * 1000 * 1000 + td.microseconds


def bench_search(query_file, index):
  with open(query_file) as ifp:
    for line in ifp:
      field_id, query = line.strip().split('|', 2)
      count = 0
      start = datetime.now()
      qbody = {'query': {'match': {'field%s' % field_id: query}}}
      res = es.search(index=index, body=qbody, fields=[], size=100000)
      for _ in res['hits']['hits']:
        count += 1
      end = datetime.now()
      print '%d\t%d' % (count, us(end - start))


def bench_get(record_count, index, doc_type):
  ids = random.sample(range(0, record_count), min(100000, record_count))
  for i in ids:
    start = datetime.now()
    res = es.get(index=index, doc_type=doc_type, id=i)
    length = len(res['_source'])
    end = datetime.now()
    print '%d\t%s\t%d' % (i, length, us(end - start))


def main(argv):
  es_server = 'localhost'
  query_file = ''
  index = 'bench'
  doc_type = 'data'
  bench_type = 'search'
  help_msg = 'esbench.py -e <es-server> -q <queries> -i <index> -t <doc-type> -b <bench-type>'
  try:
    opts, args = getopt.getopt(argv, 'he:q:i:t:b:', ['es-server', 'queries=', 'index=', 'type=', 'benchtype='])
  except getopt.GetoptError:
    print help_msg
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print help_msg
      sys.exit()
    elif opt in ('-e', '--es-server'):
      es_server = arg
    elif opt in ('-q', '--queries'):
      query_file = arg
    elif opt in ('-i', '--index'):
      index = arg
    elif opt in ('-t', '--type'):
      doc_type = arg
    elif opt in ('-b', '--benchtype'):
      bench_type = arg
  if bench_type == 'search' and query_file == '':
    print 'Error: Must specify query-file for search benchmark!'
    sys.exit(2)

  host = 'http://%s:9200' % es_server
  global es
  es = Elasticsearch(hosts=[host], timeout=600)
  record_count = es.count(index=index)['count']

  if bench_type == 'search':
    bench_search(query_file, index)
  elif bench_type == 'get':
    bench_get(record_count, index, doc_type)


if __name__ == '__main__':
  main(sys.argv[1:])
