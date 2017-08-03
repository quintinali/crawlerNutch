package pd.nutch.ranking.webpage;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

//import pd.nutch.lite.mixer.mapping.wikipedia.wikiminer.WikiMinerMap;
import pd.nutch.lite.model.Term;
import pd.nutch.lite.algorithms.ranked.unsupervised.cvalue.Candidate;
import pd.nutch.lite.model.Document;
import pd.nutch.lite.algorithms.ranked.unsupervised.cvalue.ProcessLinguisticFilters;
import pd.nutch.lite.algorithms.ranked.unsupervised.cvalue.filters.english.NounFilter;
import pd.nutch.lite.parsers.english.PlainTextDocumentReaderIXAEn;
import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;
import pd.nutch.main.CrawlerAbstract;
import pd.nutch.main.CrawlerConstants;
import pd.nutch.main.CrawlerEngine;
import pd.nutch.partition.KGreedyPartitionSolver;
import pd.nutch.partition.ThePartitionProblemSolver;
import pd.nutch.partition.logPartitioner;
import scala.Tuple2;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class TermExtration extends CrawlerAbstract {
  String indexName;
  String typeName;
  String termindex;
  String termcandidateType;
  String candidateStaticType;
  transient SparkContext sc = null;
  String path = props.getProperty(CrawlerConstants.FILE_PATH);
  int partition = 64;

  List<String> standardStop = null;

  public TermExtration(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
    indexName = props.getProperty(CrawlerConstants.ES_INDEX_NAME);
    typeName = props.getProperty(CrawlerConstants.CRAWLER_TYPE_NAME);
    termindex = props.getProperty(CrawlerConstants.TERM_INDEX);
    termcandidateType = props.getProperty(CrawlerConstants.TERM_CANDIDATE_TYPE);
    candidateStaticType = props.getProperty(CrawlerConstants.TERM_CANDIDATE_STATIC_TYPE);
  }

  public void addCandidateMapping() {
    // add index
    boolean exists = es.getClient().admin().indices().prepareExists(termindex).execute().actionGet().isExists();
    if (exists) {
      return;
    }
    es.getClient().admin().indices().prepareCreate(termindex).execute().actionGet();

    // add mapping
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(props.getProperty(CrawlerConstants.TERM_CANDIDATE_TYPE)).startObject("properties").startObject("text").field("type", "string")
          .field("index", "not_analyzed").endObject().startObject("length").field("type", "integer").endObject().endObject().endObject().endObject();
      es.getClient().admin().indices().preparePutMapping(props.getProperty(CrawlerConstants.TERM_INDEX)).setType(props.getProperty(CrawlerConstants.TERM_CANDIDATE_TYPE)).setSource(Mapping).execute()
          .actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void extractTermCandidates() throws InterruptedException, ExecutionException, ElasticsearchException, IOException {

    this.addCandidateMapping();

    Map<String, List<String>> hostNoiseMap = this.loadContentNoise();
    JavaRDD<String> hostRDD = this.getHostRDD();
    int hostCount = 0;
    hostCount = hostRDD.mapPartitions((FlatMapFunction<Iterator<String>, Integer>) iterator -> {
      // TODO Auto-generated method stub
      ESDriver tmpES = new ESDriver(props);
      tmpES.createBulkProcesser();
      List<Integer> realHostNums = new ArrayList<Integer>();
      while (iterator.hasNext()) {
        String host = iterator.next();
        List<String> noises = hostNoiseMap.get(host);
        Document doc = new Document("", "");
        PlainTextDocumentReaderIXAEn docParser = new PlainTextDocumentReaderIXAEn();
        if (noises != null) {
          docParser.addNosieSentences(noises);
        }

        ProcessLinguisticFilters filters = new ProcessLinguisticFilters();
        filters.addFilter(new NounFilter());

        Integer realHost = extractCandidatePerHost(tmpES, host, doc, docParser, filters);
        realHostNums.add(realHost);
      }
      tmpES.destroyBulkProcessor();
      tmpES.close();
      return realHostNums.iterator();
    }).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);
  }

  public void testExtractTermPerHost() throws IOException {
    Map<String, List<String>> hostNoiseMap = this.loadContentNoise();
    es.createBulkProcesser();

    String host = "amp.space.com";
    List<String> noises = hostNoiseMap.get(host);
    Document doc = new Document("", "");
    PlainTextDocumentReaderIXAEn docParser = new PlainTextDocumentReaderIXAEn();
    docParser.addNosieSentences(noises);

    ProcessLinguisticFilters filters = new ProcessLinguisticFilters();
    filters.addFilter(new NounFilter());

    extractCandidatePerHost(es, host, doc, docParser, filters);

    es.destroyBulkProcessor();

  }

  public Integer extractCandidatePerHost(ESDriver es, String host, Document doc, PlainTextDocumentReaderIXAEn docParser, ProcessLinguisticFilters filters) {

    TermQueryBuilder termquery = QueryBuilders.termQuery("host", host);
    SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(indexName).setTypes(typeName).setScroll(new TimeValue(300000)).setQuery(termquery).setSize(50).addSort("nutch_score",
        SortOrder.DESC);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();

        String id = (String) result.get("url");
        String content = (String) result.get("content");
        String inlinks = (String) result.get("anchor_inlinks");
        String page = inlinks.replaceAll("&&", ". ") + content;

        docParser.reset();
        docParser.readSource(page);
        doc.setSentenceList(docParser.getSentenceList());
        doc.setTokenList(docParser.getTokenizedSentenceList());
        List<Candidate> candList = filters.processText(doc.getTokenList());
        int candNum = candList.size();
        for (int i = 0; i < candNum; i++) {
          Candidate cand = candList.get(i);
          IndexRequest ir = null;
          try {
            ir = new IndexRequest(termindex, termcandidateType).source(jsonBuilder().startObject().field("text", cand.getText())/*.field("length", cand.getLenght())*/.endObject());
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          es.getBulkProcessor().add(ir);
        }

        // for test
        /*System.out.println(page);
        
        System.out.println("sentence");
        System.out.println(doc.getSentenceList().size());
        System.out.println(doc.getSentenceList());
        
        System.out.println("tokens");
        System.out.println(doc.getTokenList().size());
        System.out.println(doc.getTokenList().toString());
        
        System.out.println("candidates");
        System.out.println(candList.size());
        for (int i = 0; i < candList.size(); i++) {
          System.out.println(candList.get(i).toString());
        }
        System.out.println("");*/
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return 1;
  }

  private JavaRDD<String> getHostRDD() {
    Map<String, Double> hostDocs = getHostDocs();
    JavaRDD<String> hostRDD = parallizeHosts(hostDocs);
    return hostRDD;
  }

  private Map<String, Double> getHostDocs() {
    Terms hosts = this.getHostTerms();
    Map<String, Double> hostList = new HashMap<String, Double>();
    for (Terms.Bucket entry : hosts.getBuckets()) {
      String ip = (String) entry.getKey();
      Long count = entry.getDocCount();
      hostList.put(ip, Double.valueOf(count));
    }

    return hostList;
  }

  private JavaRDD<String> parallizeHosts(Map<String, Double> hostDocs) {

    // prepare list for parallize
    List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
    for (String user : hostDocs.keySet()) {
      list.add(new Tuple2<String, Double>(user, hostDocs.get(user)));
    }

    // group users
    ThePartitionProblemSolver solution = new KGreedyPartitionSolver();
    Map<String, Integer> UserGroups = solution.solve(hostDocs, this.partition);

    JavaPairRDD<String, Double> pairRdd = spark.sc.parallelizePairs(list);
    JavaPairRDD<String, Double> userPairRDD = pairRdd.partitionBy(new logPartitioner(UserGroups, this.partition));

    // repartitioned user RDD
    JavaRDD<String> hostRDD = userPairRDD.keys();
    // checkUserPartition(userRDD);

    return hostRDD;
  }

  private Terms getHostTerms() {
    TermQueryBuilder termquery = QueryBuilders.termQuery("lang", "en");
    SearchResponse sr = es.getClient().prepareSearch(indexName).setTypes(typeName).setQuery(termquery).setSize(0).addAggregation(AggregationBuilders.terms("Hosts").field("host").size(0)).execute()
        .actionGet();
    Terms hosts = sr.getAggregations().get("Hosts");
    return hosts;
  }

  private Map<String, List<String>> loadContentNoise() throws IOException {
    String contentNoise = path + "contentNoise.txt";
    Map<String, List<String>> hostNoiseMap = new HashMap<String, List<String>>();

    BufferedReader br = null;
    String line = "";

    br = new BufferedReader(new FileReader(contentNoise));
    boolean bNewHost = true;
    String host = "";
    List<String> noises = new ArrayList<String>();
    while ((line = br.readLine()) != null) {
      if (bNewHost) {
        if (!host.equals("") && noises.size() > 0) {
          hostNoiseMap.put(host, noises);
        }

        host = line;
        noises = new ArrayList<String>();
        bNewHost = false;
      } else {
        if (!line.equals("")) {
          noises.add(line);
        } else {
          bNewHost = true;
        }
      }
    }

    return hostNoiseMap;
  }

  public void candidateStatistic() {

    es.deleteType(termindex, candidateStaticType);
    es.createBulkProcesser();

    SearchResponse sr = es.getClient().prepareSearch(termindex).setTypes(termcandidateType).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("candidates").field("text").size(0)).execute().actionGet();
    Terms candidates = sr.getAggregations().get("candidates");

    for (Terms.Bucket entry : candidates.getBuckets()) {
      String candidate = (String) entry.getKey();
      long freq = entry.getDocCount();
      long length = candidate.split("\\s").length;

      IndexRequest ir = null;
      try {
        ir = new IndexRequest(termindex, candidateStaticType).source(jsonBuilder().startObject().field("text_analysis", candidate).field("freq", freq).field("length", length).endObject());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      es.getBulkProcessor().add(ir);
    }

    es.destroyBulkProcessor();
  }

  public void candidatesCValue() {
    es.createBulkProcesser();

    SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(termindex).setTypes(candidateStaticType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String text = (String) result.get("text_analysis");
        int length = (int) result.get("length");
        int freq = (int) result.get("freq");

        boolean aux = this.applyStopwordList(text);
        if (aux) {
          DeleteRequest deleteRequest = new DeleteRequest(termindex, candidateStaticType, hit.getId());
          es.getBulkProcessor().add(deleteRequest);
          continue;
        }

        Candidate cand = this.candidateCValue(text, length, freq);
        UpdateRequest ur = null;
        try {
          ur = new UpdateRequest(termindex, candidateStaticType, hit.getId())
              .doc(jsonBuilder().startObject().field("cvalue", cand.getCValue()).field("uniqNesters", cand.getNesterCount()).field("freqNested", cand.getFreqNested()).endObject());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        es.getBulkProcessor().add(ur);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

  public void initStopList() {
    try {
      standardStop = Files.readAllLines(Paths.get("resources/lite/stopWordLists/standardStopList"), StandardCharsets.UTF_8);
    } catch (IOException e1x) {
      System.err.println("Check your resources dir: " + e1x.getMessage());
    }
  }

  public boolean applyStopwordList(String term) {
    if (standardStop == null) {
      this.initStopList();
    }

    boolean aux = false;
    for (String s : standardStop) {
      if (s.equals(standardStop)) {
        aux = true;
        break;
      }
    }

    return aux;
  }

  public Candidate candidateCValue(String text, int length, int freq) {

    Candidate cand = new Candidate(text, length);
    cand.incrementFreq(freq);

    BoolQueryBuilder qb = new BoolQueryBuilder();
    qb.should(QueryBuilders.multiMatchQuery(text, "text_analysis").type(MultiMatchQueryBuilder.Type.PHRASE).tieBreaker((float) 0.5));

    SearchRequestBuilder searchBuilder = es.getClient().prepareSearch(termindex).setTypes(candidateStaticType).setQuery(qb).setSize(500);
    SearchResponse response = searchBuilder.execute().actionGet();
    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String nest_text = (String) result.get("text_analysis");
      int nest_len = (int) result.get("length");
      int nest_freq = (int) result.get("freq");
      if (!nest_text.equals(text)) {
        cand.observeNested();
        cand.incrementFreqNested(nest_freq);
      }
    }

    cand.getCValue();
    return cand;
  }

  public void removeAndMixTerms(float mincvalue) throws IOException {

    List<Term> mixedTermList = new ArrayList<Term>();

    String path = props.getProperty(CrawlerConstants.FILE_PATH);
    String fileName = path + "mixterms.txt";
    FileWriter fw = this.createFile(fileName);
    BufferedWriter bw = new BufferedWriter(fw);

    BoolQueryBuilder qb = new BoolQueryBuilder();
    qb.must(QueryBuilders.rangeQuery("cvalue").gt(mincvalue));
    qb.must(QueryBuilders.rangeQuery("length").lt(7));

    SearchRequestBuilder scrollBuilder = es.getClient().prepareSearch(termindex).setTypes(candidateStaticType).setScroll(new TimeValue(60000)).setQuery(qb).addSort("cvalue", SortOrder.DESC)
        .setSize(100);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String text = (String) result.get("text_analysis");
        double cvalue = (double) result.get("cvalue");
        try {
          bw.write(text + "&&&&" + cvalue + "\n");
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        Term term = new Term(text, (float) cvalue);
        mixedTermList.add(term);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    Document doc = new Document("", "");
    doc.setMixedTermList(mixedTermList);

    bw.close();
    fw.close();
    // wike related

    /* WikiminnerHelper helper = WikiminnerHelper.getInstance(resources);
    helper.setLanguage(lang);
    // we may operate in local mode (using Wikiminer as API instead of
    // interacting via REST api
    helper.setLocalMode(props.getProperty("localMode").equals("true"), "/home/angel/nfs/wikiminer/configs/wikipedia");
    WikiMinerMap wikimapping = new WikiMinerMap(resources, helper);
    wikimapping.mapCorpus(doc);
    disambiguator.disambiguateTopics(doc);
    // we may disambiguate topics that do not disambiguated correctly
    DuplicateRemoval.disambiguationRemoval(doc);
    DuplicateRemoval.topicDuplicateRemoval(doc);
    // obtain the wiki links,labels, etc
    data.processDocument(doc);
    // measure domain relatedness
    relate.relate(doc);
    // save the results
    Document.saveJsonToDir(outDir, doc);*/
  }

  public void getFinalTerms() throws IOException {
    String file = path + "pageContents.json";

    InputStream is= new FileInputStream(file);
    String jsonTxt = IOUtils.toString(is);
    JsonParser parser = new JsonParser();
    JsonObject termObj = parser.parse(jsonTxt).getAsJsonObject();
    
    System.out.println(termObj.get("topics").isJsonArray());
    
    JsonArray topicArray = termObj.get("topics").getAsJsonArray();
    int topic_length = topicArray.size();
    for (int i = 0; i < topic_length; i++) {
      JsonObject topic = topicArray.get(i).getAsJsonObject();
      System.out.println(topic.get("topic"));
    }
    
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    CrawlerEngine me = new CrawlerEngine();
    me.loadConfig();
    SparkDriver spark = new SparkDriver(me.getConfig());
    ESDriver es = new ESDriver(me.getConfig());

    TermExtration term = new TermExtration(me.loadConfig(), es, spark);
    try {

      //term.extractTermCandidates();
      term.candidateStatistic();
      term.candidatesCValue();
      term.removeAndMixTerms(1.9f);
      // term.candidateCValue("Descent Image", 2, 102);
      term.getFinalTerms();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
