#!/usr/bin/python

import os
import sys
import getopt
import random
import socket
import threading
from datetime import datetime
from elasticsearch import Elasticsearch

writeLock = threading.Lock()

if os.name != "nt":
    import fcntl
    import struct

    def get_interface_ip(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))[20:24])


def get_ip():
    ip = socket.gethostbyname(socket.gethostname())
    if ip.startswith("127.") and os.name != "nt":
        interfaces = [
            "eth0",
            "eth1",
            "eth2",
            "wlan0",
            "wlan1",
            "wifi0",
            "ath0",
            "ath1",
            "ppp0",
            ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    return ip


def secs(td):
  return (float(td.days) * 24.0 * 60.0 * 60.0 + float(td.seconds)) + float(td.microseconds) / (1000.0 * 1000.0)


class BenchmarkThread(threading.Thread):
  def __init__(self, thread_id, bench_type, es_server, index, doc_type, queries, record_count):
    threading.Thread.__init__(self)
    self.thread_id = thread_id
    self.bench_type = bench_type
    self.index = index
    self.doc_type = doc_type
    self.queries = queries
    self.record_count = record_count
    print '[Thread %d] Connecting to ES...' % thread_id
    self.es = Elasticsearch(hosts=['http://%s:9200' % es_server], timeout=600)
    print '[Thread %d] Connected.' % thread_id
    self.query_count = len(queries)
    self.WARMUP_TIME = 60
    self.MEASURE_TIME = 120
    self.COOLDOWN_TIME = 60

  def bench_get(self):
    print '[Thread %d] Benchmarking get...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id
    return throughput

  def bench_search(self):
    print '[Thread %d] Benchmarking search...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      count = 0
      res = self.es.search(index=self.index, body=query, fields=[], size=10000)
      for _ in res['hits']['hits']:
        count += 1
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      count = 0
      res = self.es.search(index=self.index, body=query, fields=[], size=10000)
      for _ in res['hits']['hits']:
        count += 1
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      count = 0
      res = self.es.search(index=self.index, body=query, fields=[], size=10000)
      for _ in res['hits']['hits']:
        count += 1
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id

    return throughput
    
  def bench_search_append(self):
    print '[Thread %d] Benchmarking search+append...' % self.thread_id

    qid = 0
    aid = self.record_count + 1

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      if qid % 20 == 0:
        self.es.index(index=self.index, doc_type=self.doc_type, id=str(aid), body=query)
        aid += 1
      else:
        count = 0
        res = self.es.search(index=self.index, body=query, fields=[], size=10000)
        for _ in res['hits']['hits']:
          count += 1
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      if qid % 20 == 0:
        self.es.index(index=self.index, doc_type=self.doc_type, id=str(aid), body=query)
        aid += 1
      else:
        count = 0
        res = self.es.search(index=self.index, body=query, fields=[], size=10000)
        for _ in res['hits']['hits']:
          count += 1
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      if qid % 20 == 0:
        self.es.index(index=self.index, doc_type=self.doc_type, id=str(aid), body=query)
        aid += 1
      else:
        count = 0
        res = self.es.search(index=self.index, body=query, fields=[], size=10000)
        for _ in res['hits']['hits']:
          count += 1
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id

    return throughput
    
  def bench_get_append(self):
    print '[Thread %d] Benchmarking get+append...' % self.thread_id

    qid = 0
    aid = self.record_count + 1

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      if qid % 20 == 0:
        self.es.index(index=self.index, doc_type=self.doc_type, id=str(aid), body=query)
        aid += 1
      else:
        count = 0
        self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      if qid % 20 == 0:
        self.es.index(index=self.index, doc_type=self.doc_type, id=str(aid), body=query)
        aid += 1
      else:
        count = 0
        self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      if qid % 20 == 0:
        self.es.index(index=self.index, doc_type=self.doc_type, id=str(aid), body=query)
        aid += 1
      else:
        count = 0
        self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id

    return throughput

  def bench_get_search(self):
    print '[Thread %d] Benchmarking get+search...' % self.thread_id

    qid = 0

    # Warmup
    print '[Thread %d] Warmup phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.WARMUP_TIME:
      query = self.queries[qid]
      if qid % 2 == 0:
        query = self.queries[qid]
        self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      else:
        count = 0
        res = self.es.search(index=self.index, body=query, fields=[], size=10000)
        for _ in res['hits']['hits']:
          count += 1
      qid = (qid + 1) % self.query_count

    # Measure
    print '[Thread %d] Measure phase...' % self.thread_id
    query_count = 0
    start = datetime.now()
    while secs(datetime.now() - start) < self.MEASURE_TIME:
      query = self.queries[qid]
      if qid % 2 == 0:
        query = self.queries[qid]
        self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      else:
        count = 0
        res = self.es.search(index=self.index, body=query, fields=[], size=10000)
        for _ in res['hits']['hits']:
          count += 1
      qid = (qid + 1) % self.query_count
      query_count += 1

    total_time = secs(datetime.now() - start)
    throughput = float(query_count) / total_time

    # Cooldown
    print '[Thread %d] Cooldown phase...' % self.thread_id
    start = datetime.now()
    while secs(datetime.now() - start) < self.COOLDOWN_TIME:
      query = self.queries[qid]
      if qid % 2 == 0:
        query = self.queries[qid]
        self.es.get(index=self.index, doc_type=self.doc_type, id=query)
      else:
        count = 0
        res = self.es.search(index=self.index, body=query, fields=[], size=10000)
        for _ in res['hits']['hits']:
          count += 1
      qid = (qid + 1) % self.query_count

    print '[Thread %d] Benchmark complete.' % self.thread_id

    return throughput

  def run(self):
    if self.bench_type == 'get':
      throughput = self.bench_get()
    elif self.bench_type == 'search':
      throughput = self.bench_search()
    elif self.bench_type == 'search-append':
      throughput = self.bench_search_append()
    elif self.bench_type == 'get-append':
      throughput = self.bench_get_append()
    elif self.bench_type == 'get-search':
      throughput = self.bench_get_search()
    else:
      print '[Thread %d] Error: Invalid bench_type %s.' % (self.thread_id, self.bench_type)
      sys.exit(2)

    writeLock.acquire()
    with open('thput', 'a') as out:
      out.write('%d\t%.2f\n' % (self.thread_id, throughput))
    writeLock.release()


def csv2json(csv):
  json = {}
  fields = csv.split('|')
  for i in range(0, len(fields)):
    json['field%d' % i] = fields[i]
  return json


def load_queries(bench_type, query_file, append_file, record_count):
  queries = []
  appends = []
  print '[Main Thread] Loading queries...'
  if bench_type == 'search':
    
    if query_file == '':
      print 'Error: Must specify query-file for search benchmark!'
      sys.exit(2)
    with open(query_file) as ifp:
      for line in ifp:
        field_id, query = line.strip().split('|', 2)
        qbody = {'query': {'match': {'field%s' % field_id: query}}}
        queries.append(qbody)
    queries = random.sample(queries, min(100000, len(queries)))
    
  elif bench_type == 'get':
    
    queries = [random.randrange(0, record_count) for _ in range(min(100000, record_count))]
    
  elif bench_type == 'search-append':
    
    search_queries = []
    if query_file == '':
      print 'Error: Must specify query-file for search benchmark!'
      sys.exit(2)
    with open(query_file) as ifp:
      for line in ifp:
        field_id, query = line.strip().split('|', 2)
        qbody = {'query': {'match': {'field%s' % field_id: query}}}
        search_queries.append(qbody)
    search_queries = random.sample(search_queries, min(100000, len(search_queries)))
    
    append_queries = []
    if append_file == '':
      print 'Error: Must specify append-file for search benchmark!'
      sys.exit(2)
    with open(append_file) as ifp:
      for line in ifp:
        doc = csv2json(line.rstrip())
        append_queries.append(doc)
    append_queries = random.sample(append_queries, min(100000, len(append_queries)))
    
    aid = 0
    sid = 0
    query_count = 100000
    for i in range(0, query_count):
      if i % 20 == 0:
        queries.append(append_queries[aid % len(append_queries)])
        aid += 1
      else:
        queries.append(search_queries[sid % len(search_queries)])
        sid += 1
        
  elif bench_type == 'get-append':
    
    get_queries = [random.randrange(0, record_count) for _ in range(min(100000, record_count))]
    
    append_queries = []
    if append_file == '':
      print 'Error: Must specify append-file for search benchmark!'
      sys.exit(2)
    with open(append_file) as ifp:
      for line in ifp:
        doc = csv2json(line.rstrip())
        append_queries.append(doc)
    append_queries = random.sample(append_queries, min(100000, len(append_queries)))
    
    aid = 0
    gid = 0
    query_count = 100000
    for i in range(0, query_count):
      if i % 20 == 0:
        queries.append(append_queries[aid % len(append_queries)])
        aid += 1
      else:
        queries.append(get_queries[gid % len(get_queries)])
        gid += 1
    
  elif bench_type == 'get-search':
    
    get_queries = random.sample(range(0, record_count), min(100000, record_count))
    
    search_queries = []
    if query_file == '':
      print 'Error: Must specify query-file for search benchmark!'
      sys.exit(2)
    with open(query_file) as ifp:
      for line in ifp:
        field_id, query = line.strip().split('|', 2)
        qbody = {'query': {'match': {'field%s' % field_id: query}}}
        search_queries.append(qbody)
    search_queries = random.sample(search_queries, min(100000, len(search_queries)))

    query_count = max(len(get_queries), len(search_queries)) * 2
    for i in range(0, query_count):
      if i % 2 == 0:
        queries.append(get_queries[(i / 2) % len(get_queries)])
      else:
        queries.append(search_queries[(i / 2) % len(search_queries)])
  else:
    print 'Error: Invalid benchtype %s' % bench_type

  return queries


def main(argv):
  es_server = get_ip()
  query_file = ''
  append_file = ''
  index = 'bench'
  doc_type = 'data'
  bench_type = 'search'
  num_threads = 1
  help_msg = 'esthroughput.py -e <es-server> -q <queries> -a <appends> -i <index> -t <doc-type> -b <bench-type> -n <num-threads>'
  try:
    opts, args = getopt.getopt(argv, 'he:q:a:i:t:b:n:',
                               ['es-server', 'queries=', 'appends=', 'index=', 'type=', 'benchtype=', 'numthreads='])
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
    elif opt in ('-a', '--appends'):
      append_file = arg
    elif opt in ('-i', '--index'):
      index = arg
    elif opt in ('-t', '--type'):
      doc_type = arg
    elif opt in ('-b', '--benchtype'):
      bench_type = arg
    elif opt in ('-n', '--numthreads'):
      num_threads = int(arg)

  es = Elasticsearch(hosts=['http://%s:9200' % es_server], timeout=600)
  count = es.count(index=index)['count']
  del es

  threads = []
  print '[Main Thread] Initializing %d threads...' % num_threads
  for i in range(0, num_threads):
    queries = load_queries(bench_type=bench_type, query_file=query_file, append_file=append_file, record_count=count)
    thread = BenchmarkThread(thread_id=i, bench_type=bench_type, es_server=es_server, index=index, doc_type=doc_type,
                             queries=queries, record_count=count)
    threads.append(thread)

  print '[Main Thread] Starting threads...'
  for thread in threads:
    thread.start()

  print '[Main Thread] Waiting for threads to join...'
  for thread in threads:
    thread.join()


if __name__ == '__main__':
  main(sys.argv[1:])
