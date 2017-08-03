package pd.nutch.ranking.webpage;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.lite.algorithms.ranked.unsupervised.cvalue.ProcessLinguisticFilters;
import pd.nutch.lite.algorithms.ranked.unsupervised.cvalue.filters.english.NounFilter;
import pd.nutch.lite.model.Document;
import pd.nutch.lite.parsers.AbstractDocumentReader;
import pd.nutch.lite.parsers.english.PlainTextDocumentReaderIXAEn;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;

public class cleanWebPages extends CrawlerAbstract {

  String indexName;
  String typeName;
  String tmpIndex;

  int sampleSize = 500;
  double rate = 0.1;

  transient SparkContext sc = null;
  String path = props.getProperty(CrawlerConstants.FILE_PATH);
  AbstractDocumentReader parser = null;

  public cleanWebPages(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
    indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
    typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
    tmpIndex = "tmp_" + indexName;

    parser = new PlainTextDocumentReaderIXAEn();
  }

  public void selectSamplePages() throws IOException {

    this.initCrawler(tmpIndex);
    es.createBulkProcesser();
    SearchResponse sr = es.getClient().prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("hosts").field("host").size(0)).execute().actionGet();
    Terms hosts = sr.getAggregations().get("hosts");

    for (Terms.Bucket entry : hosts.getBuckets()) {
      String host = (String) entry.getKey();

      BoolQueryBuilder qb = new BoolQueryBuilder();
      qb.must(QueryBuilders.termQuery("host", host));
      qb.must(QueryBuilders.termQuery("lang", "en"));

      SearchRequestBuilder searchBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName).setQuery(qb).setSize(sampleSize);
      SearchResponse scrollResp = searchBuilder.execute().actionGet();

      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String id = (String) result.get("url");
        String pagehost = (String) result.get("host");
        String content = (String) result.get("content");
        String title = (String) result.get("title");
        String lang = (String) result.get("lang");

        parser.reset();
        parser.readSentences(content);
        List<String> sentences = parser.getSentenceList();

        IndexRequest ir = null;
        try {
          ir = new IndexRequest(tmpIndex, typeName).source(
              jsonBuilder().startObject().field("url", hit.getId()).field("sentences", String.join("&&&&", sentences)).field("title", title).field("host", host).field("lang", lang).endObject());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        es.getBulkProcessor().add(ir);
      }
    }

    es.destroyBulkProcessor();
  }

  public void analyzeWebPage(String fileName, String field) throws IOException {
    FileWriter fw = this.createFile(fileName);
    BufferedWriter bw = new BufferedWriter(fw);

    SearchRequestBuilder sr2Builder = es.getClient().prepareSearch(tmpIndex).setTypes(typeName).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("hostAgg").field("host").size(0).subAggregation(AggregationBuilders.terms("sentencesAgg").field(field).size(0)));

    SearchResponse sr2 = sr2Builder.execute().actionGet();
    Terms hosts = sr2.getAggregations().get("hostAgg");
    for (Terms.Bucket host : hosts.getBuckets()) {
      long hostPages = host.getDocCount();
      if (hostPages < sampleSize * 0.05) {
        break;
      }
      bw.write(host.getKey() /*+ " "  + hostPages*/ + "\n");
      double minOccurance = hostPages * rate;
      Terms sentAgg = host.getAggregations().get("sentencesAgg");
      int distinctSentence = sentAgg.getBuckets().size();
      for (Terms.Bucket senEntry : sentAgg.getBuckets()) {
        String sentence = senEntry.getKey().toString();
        long count = senEntry.getDocCount();
        if (count > minOccurance) {
          bw.write(sentence /*+ " : " + count*/ + "\n");
        }
      }

      bw.write("\n");
    }

    bw.close();
    fw.close();
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    CrawlerEngine me = new CrawlerEngine();
    me.loadConfig();
    SparkDriver spark = new SparkDriver(me.getConfig());
    ESDriver es = new ESDriver(me.getConfig());

    cleanWebPages clean = new cleanWebPages(me.loadConfig(), es, spark);
    try {
      clean.selectSamplePages();
      es.refreshIndex();
      String path = me.getConfig().getProperty(CrawlerConstants.FILE_PATH);
      clean.analyzeWebPage(path + "contentNoise.txt", "sentences");
      clean.analyzeWebPage(path + "titleNoise.txt", "title");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
