package com.anuragkh;


import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchBenchmark {

  private String dataPath;
  private String attrPath;
  private AtomicLong currentKey;
  private String hostname;

  private int[] timestamps;
  private String[] srcips;
  private String[] dstips;
  private int[] sports;
  private int[] dports;
  private byte[][] datas;

  private Logger LOG = Logger.getLogger(ElasticsearchBenchmark.class.getName());

  static final long REPORT_RECORD_INTERVAL = 10000;

  public ElasticsearchBenchmark(String hostname, String dataPath, String attrPath) {

    this.hostname = hostname;
    this.dataPath = dataPath;
    this.attrPath = attrPath;
    this.currentKey = new AtomicLong(0L);

    loadData();

    LOG.info("Initialization complete.");
  }

  private TransportClient createClient() {
    TransportClient client = null;
    try {
      Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").build();
      client = TransportClient.builder().settings(settings).build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), 9300));
    } catch (UnknownHostException e) {
      LOG.severe("Unknown host exception: " + e.getMessage());
    }
    return client;
  }

  XContentBuilder insertOp(long id, int ts, String sIP, String dIP, int sPort, int dPort,
    byte[] data) {

    XContentBuilder builder = null;
    try {
      builder = jsonBuilder().startObject().field("ts", ts).field("srcip", sIP).field("dstip", dIP)
        .field("sport", sPort).field("dport", dPort).field("data", data);
    } catch (IOException e) {
      LOG.info("Error creating json for packet id " + id);
      System.exit(0);
    }
    return builder;
  }

  private int countLines() {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(attrPath));
    } catch (FileNotFoundException e) {
      LOG.severe("Error: " + e.getMessage());
      System.exit(-1);
    }
    int lines = 0;
    try {
      while (reader.readLine() != null)
        lines++;
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return lines;
  }

  private void loadData() {
    // Generate queries
    LOG.info("Loading packet data into memory...");

    BufferedInputStream dataStream;
    BufferedReader attrReader;

    // Allocate space for packet data
    int numPackets = countLines();
    timestamps = new int[numPackets];
    srcips = new String[numPackets];
    dstips = new String[numPackets];
    sports = new int[numPackets];
    dports = new int[numPackets];
    datas = new byte[numPackets][];

    try {
      dataStream = new BufferedInputStream(new FileInputStream(dataPath));
      attrReader = new BufferedReader(new FileReader(attrPath));
      String attrLine;
      int i = 0;
      while ((attrLine = attrReader.readLine()) != null) {
        String[] attrs = attrLine.split("\\s+");
        if (attrs.length != 6) {
          LOG.severe("Error parsing attribute line: " + attrLine);
          System.exit(-1);
        }
        timestamps[i] = Integer.parseInt(attrs[0]);
        int length = Integer.parseInt(attrs[1]);
        srcips[i] = attrs[2];
        dstips[i] = attrs[3];
        sports[i] = Integer.parseInt(attrs[4]);
        dports[i] = Integer.parseInt(attrs[5]);
        datas[i] = new byte[length];
        int nbytes = dataStream.read(datas[i]);
        if (nbytes != length) {
          LOG.severe("Error reading data: Length " + length + " does not match num bytes read.");
          System.exit(-1);
        }
        i++;
      }
    } catch (FileNotFoundException e) {
      LOG.severe("File not found: " + e.getMessage());
      System.exit(0);
    } catch (IOException e) {
      LOG.severe("I/O Exception occurred: " + e.getMessage());
      System.exit(0);
    }
    LOG.info("Loaded packet data in memory.");
  }

  class ProgressLogger {
    private BufferedWriter out;

    public ProgressLogger(String fileName) {
      try {
        out = new BufferedWriter(new FileWriter(fileName));
      } catch (IOException e) {
        LOG.severe("I/O Exception occurred: " + e.getMessage());
        System.exit(0);
      }
    }

    public synchronized void logProgress(long numOps) {
      try {
        out.write(System.currentTimeMillis() + " " + numOps + "\n");
      } catch (IOException e) {
        LOG.severe("I/O Exception occurred: " + e.getMessage());
        System.exit(0);
      }
    }

    public void close() {
      try {
        out.close();
      } catch (IOException e) {
        LOG.severe("I/O Exception occurred: " + e.getMessage());
        System.exit(0);
      }
    }
  }


  class LoaderThread extends Thread {
    private int index;
    private TransportClient session;
    private int localOpsProcessed;
    private long timebound;
    private double throughput;
    private ProgressLogger logger;

    public LoaderThread(int index, long timebound, ProgressLogger logger) {
      this.index = index;
      this.timebound = timebound * 1000;
      this.session = createClient();
      this.logger = logger;
      this.localOpsProcessed = 0;
      this.throughput = 0.0;
    }

    public int getIndex() {
      return index;
    }

    public double getThroughput() {
      return throughput;
    }

    private int executeOne() {
      long id = currentKey.getAndAdd(1L);
      int i = (int) id;
      if (i >= timestamps.length)
        return -1;
      XContentBuilder entry =
        insertOp(id, timestamps[i], srcips[i], dstips[i], sports[i], dports[i], datas[i]);
      IndexResponse response =
        session.prepareIndex("bench", "packets", String.valueOf(id)).setSource(entry).get();
      localOpsProcessed++;

      assert Integer.parseInt(response.getId()) == i;
      assert response.isCreated();

      return i + 1;
    }

    @Override public void run() {
      int totOpsProcessed = 0;
      long measureStart = System.currentTimeMillis();
      while (totOpsProcessed != -1 && System.currentTimeMillis() - measureStart < timebound) {
        totOpsProcessed = executeOne();
        if (totOpsProcessed % REPORT_RECORD_INTERVAL == 0) {
          logger.logProgress(totOpsProcessed);
        }
      }
      long measureEnd = System.currentTimeMillis();
      double totsecs = (double) (measureEnd - measureStart) / 1000.0;
      throughput = (double) localOpsProcessed / totsecs;

      session.close();
    }
  }

  public void loadPackets(int numThreads, long timebound) {

    LoaderThread[] threads = new LoaderThread[numThreads];
    ProgressLogger logger = new ProgressLogger("record_progress");

    for (int i = 0; i < numThreads; i++) {
      LOG.info("Initializing thread " + i + "...");
      threads[i] = new LoaderThread(i, timebound, logger);
      LOG.info("Thread " + i + " initialization complete.");
    }

    long startTime = System.currentTimeMillis();
    for (LoaderThread thread : threads) {
      thread.start();
    }

    String resFile = "write_throughput";
    try (BufferedWriter br = new BufferedWriter(new FileWriter(resFile))) {
      for (LoaderThread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOG.severe("Thread " + thread.getIndex() + " was interrupted: " + e.getMessage());
        }
        br.write(thread.getThroughput() + "\n");
      }
    } catch (IOException e) {
      LOG.severe("I/O exception writing to output file: " + e.getMessage());
    }
    long endTime = System.currentTimeMillis();
    LOG.info("Finished loading packets in " + (endTime - startTime) / 1000 + "s");

    logger.close();
  }
}
