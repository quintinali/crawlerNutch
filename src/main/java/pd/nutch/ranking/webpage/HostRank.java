package pd.nutch.ranking.webpage;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.ranking.pagerank.RankGraphFrame;
import scala.Tuple2;

public class HostRank extends CrawlerAbstract {

  String indexName;
  String typeName;
  private Map<String, String> hostMap = new HashMap<String, String>();

  public HostRank(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
    indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
    typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
  }

  public void generateHostMap() {
    SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);

    int docnum = es.getDocCount(indexName, QueryBuilders.matchAllQuery(), typeName);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String url = (String) result.get("url");
        String host = (String) result.get("host");
        if (!hostMap.containsKey(url)) {
          hostMap.put(url, host);
        }
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }
  }

  public void exportDomainUrlLinks(String hostFile, String relationFile) throws IOException {
    this.generateHostMap();

    FileWriter hostfw = this.createFile(hostFile);
    BufferedWriter hostbw = new BufferedWriter(hostfw);

    FileWriter relationfw = this.createFile(relationFile);
    BufferedWriter relationbw = new BufferedWriter(relationfw);

    SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);

    int docnum = es.getDocCount(indexName, QueryBuilders.matchAllQuery(), typeName);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String url = (String) result.get("url");
        String host = this.getDomainName(url);
        hostbw.write(host + "\n");
        String inlinks = (String) result.get("url_inlinks");
        String outlinks = (String) result.get("url_outlinks");
        if (inlinks != null && !inlinks.isEmpty()) {
          String[] inlinkArray = inlinks.split("&&&&");
          for (String inlink : inlinkArray) {
            if (!inlink.trim().isEmpty()) {
              String inhost = this.getDomainName(inlink);
              if (!inhost.isEmpty() && !host.isEmpty()) {
                // relationbw.write(inhost + " " + host + "\n");
                relationbw.write(host + " " + inhost + "\n");
                hostbw.write(inhost + "\n");
              }
            }
          }
        }

        if (outlinks != null && !outlinks.isEmpty()) {
          String[] outlinkArray = outlinks.split("&&&&");
          for (String outlink : outlinkArray) {
            if (!outlink.trim().isEmpty()) {
              String outhost = this.getDomainName(outlink);
              if (!outhost.isEmpty() && !host.isEmpty()) {
                // relationbw.write(host + " " + outhost + "\n");
                relationbw.write(outhost + " " + host + "\n");
                hostbw.write(outhost + "\n");
              }
            }
          }
        }
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    relationbw.close();
    relationfw.close();
    hostbw.close();
    hostfw.close();
  }

  public String getDomainName(String url) {
    String host = "";
    if (hostMap.containsKey(url)) {
      host = hostMap.get(url);
      return host;
    }

    URI uri = null;
    try {
      uri = new URI(url);
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      // e.printStackTrace();
    }

    if (uri == null) {
      return host;
    }
    String domain = uri.getHost();
    if (domain == null) {
      return host;
    }
    host = domain.startsWith("www.") ? domain.substring(4) : domain;
    hostMap.put(url, host);
    return host;
  }

  public void rankAndupdate(String hostFile, String relationFile, String outFileName) throws Exception {

    RankGraphFrame frame = new RankGraphFrame(props, es, spark);
    JavaPairRDD<String, Double> hostRankRDD = frame.rank(hostFile, relationFile, outFileName);
    List<Tuple2<String, Double>> hostRanks = hostRankRDD.collect();
    int hostnum = hostRanks.size();

    Map<String, Double> hostrankMap = new HashMap<String, Double>();
    for (int i = 0; i < hostnum; i++) {
      hostrankMap.put(hostRanks.get(i)._1(), hostRanks.get(i)._2());
    }

    es.createBulkProcesser();
    SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String host = (String) result.get("host");
        String id = hit.getId();

        if (hostrankMap.containsKey(host)) {
          double rank = hostrankMap.get(host);
          UpdateRequest ur = new UpdateRequest(indexName, typeName, id).doc(jsonBuilder().startObject().field("hostrank", rank).endObject());
          es.getBulkProcessor().add(ur);
        }
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

  public void run() {
    String path = props.getProperty(CrawlerConstants.FILE_PATH);
    try {
      String hostFile = path + "hosts.txt";
      String hostRelationFile = path + "hostRelations.txt";
      String hostrankFile = path + "hostRanks.txt";
      exportDomainUrlLinks(hostFile, hostRelationFile);
      rankAndupdate(hostFile, hostRelationFile, hostrankFile);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    CrawlerEngine me = new CrawlerEngine();
    me.loadConfig();
    SparkDriver spark = new SparkDriver(me.getConfig());
    ESDriver es = new ESDriver(me.getConfig());

    String path = me.loadConfig().getProperty(CrawlerConstants.FILE_PATH);
    HostRank hostrank = new HostRank(me.loadConfig(), es, spark);
    hostrank.run();
  }
}
