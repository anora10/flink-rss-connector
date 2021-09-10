package com.github.anora10.flink.rss.connector;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class FlinkRssConnectorSourceFunction extends RichParallelSourceFunction<RowData>
    implements ResultTypeQueryable<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkRssConnectorSourceFunction.class);
  private final int BLOOM_FILTER_CAPACITY = 10000;

  private final DeserializationSchema<RowData> deserializer;
  private SourceContext<RowData> context;
  private String[] uris;
  private String[] urisToProcess;
  private Integer refreshInterval;
  private volatile boolean isRunning;
  private boolean writeFilter1 = true;
  private boolean writeFilter2 = false;
  private BloomFilter<byte[]> bloomFilter1 =
      BloomFilter.create(Funnels.byteArrayFunnel(), BLOOM_FILTER_CAPACITY, 0.01);
  private BloomFilter<byte[]> bloomFilter2 =
      BloomFilter.create(Funnels.byteArrayFunnel(), BLOOM_FILTER_CAPACITY, 0.01);

  public FlinkRssConnectorSourceFunction(
      DeserializationSchema<RowData> deserializationSchema,
      String[] uris,
      Integer refreshInterval) {
    this.deserializer = deserializationSchema;
    this.uris = uris;
    this.refreshInterval = refreshInterval;
  }

  @Override
  public void open(Configuration parameters) {

    // divide uris among threads evenly -> urisToProcess is for this subtask
    final int threadCnt = getRuntimeContext().getNumberOfParallelSubtasks();
    final int uriCnt = uris.length;
    final int subtaskId = getRuntimeContext().getIndexOfThisSubtask();
    int uriToProcessCnt = uriCnt / threadCnt;
    if (uriCnt % threadCnt > subtaskId) uriToProcessCnt++;
    urisToProcess = new String[uriToProcessCnt];
    for (int i = 0; i < uriToProcessCnt; i++) {
      urisToProcess[i] = uris[i * threadCnt + subtaskId];
    }
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    LOG.info("Starting RSS source.");
    isRunning = true;
    this.context = ctx;
    while (isRunning) {
      for (String uri : urisToProcess) {
        try {
          // reading uri content
          Client client = Client.create();
          WebResource webResource = client.resource(uri);
          ClientResponse response = webResource.accept("text/plain").get(ClientResponse.class);
          if (response.getStatus() != 200) {
            throw new RuntimeException(
                "Failed at uri: " + uri + " HTTP error code : " + response.getStatus());
          }
          String responseContent = response.getEntity(String.class);

          // build XML object of response
          ByteArrayInputStream responseAsBytes =
              new ByteArrayInputStream(responseContent.getBytes(StandardCharsets.UTF_8));
          DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
          DocumentBuilder builder = factory.newDocumentBuilder();
          Document document = builder.parse(responseAsBytes);
          document.getDocumentElement().normalize();

          // process `item` tags of XML object
          NodeList itemNodeList = document.getElementsByTagName("item");
          for (int i = 0; i < itemNodeList.getLength(); i++) {
            Node itemNode = itemNodeList.item(i);
            if (itemNode.getNodeType() == Node.ELEMENT_NODE) {
              Element itemElement = (Element) itemNode;
              RowData row =
                  deserializer.deserialize(
                      RssConnectorUtils.transformXmlToString(itemElement).getBytes());
              // Bloom filtering to remove duplicates with a high probability
              if (context != null && !mightContain(row.toString().getBytes())) {
                put(row.toString().getBytes());
                context.collect(row);
              }
            }
          }
        } catch (Exception e) {
          LOG.error("Error while reading RSS source", e);
        }
      }
      Thread.sleep(refreshInterval);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  private boolean mightContain(byte[] item) {
    return bloomFilter1.mightContain(item) || bloomFilter2.mightContain(item);
  }

  private void put(byte[] item) {
    if (writeFilter1) {
      bloomFilter1.put(item);
    }
    if (writeFilter2) {
      bloomFilter2.put(item);
    }

    // filter is halfway full
    if (bloomFilter1.approximateElementCount() > BLOOM_FILTER_CAPACITY * 0.75) writeFilter2 = true;
    if (bloomFilter2.approximateElementCount() > BLOOM_FILTER_CAPACITY * 0.75) writeFilter1 = true;

    // fliter is full
    if (bloomFilter1.approximateElementCount() > BLOOM_FILTER_CAPACITY * 0.9) {
      writeFilter1 = false;
      bloomFilter1 = BloomFilter.create(Funnels.byteArrayFunnel(), BLOOM_FILTER_CAPACITY, 0.01);
    }
    if (bloomFilter2.approximateElementCount() > BLOOM_FILTER_CAPACITY * 0.9) {
      writeFilter2 = false;
      bloomFilter2 = BloomFilter.create(Funnels.byteArrayFunnel(), BLOOM_FILTER_CAPACITY, 0.01);
    }
  }
}
