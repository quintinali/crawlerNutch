package pd.nutch.crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MissingFilterBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import pd.nutch.driver.ESdriver;

public class ReindexNutchResult implements Serializable {

  /*private final String index = "nutchseedindex";
  private final String type = "doc";*/

  private final String index = "nutchseedindex";
  private final String type = "doc";

  private final String newIndex = "testindex";
  private final String newType = "testdoc";

  private Map<String, String> organizationMap = new HashMap<String, String>();

  public static ESdriver es = new ESdriver();

  public static BulkProcessor bulkProcessor = BulkProcessor
      .builder(es.client, new BulkProcessor.Listener() {
        public void beforeBulk(long executionId, BulkRequest request) {
        }

        public void afterBulk(long executionId, BulkRequest request,
            BulkResponse response) {
        }

        public void afterBulk(long executionId, BulkRequest request,
            Throwable failure) {
          System.out.println("Bulk fails!");
          throw new RuntimeException(
              "Caught exception in bulk: " + request + ", failure: " + failure,
              failure);
        }
      }).setBulkActions(1000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
      .setConcurrentRequests(1).build();

  public void reIndex()
      throws IOException, InterruptedException, ExecutionException {

    es.putMapping(this.newIndex);

    OpenNLPPhraseExtractor extrctor = new OpenNLPPhraseExtractor();

    MissingFilterBuilder filter = FilterBuilders
        .missingFilter("content_chunker_keywords");
    QueryBuilder type_query_search = QueryBuilders
        .filteredQuery(QueryBuilders.matchAllQuery(), filter);
    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(index)
        .setTypes(type).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100);

    SearchResponse scrollResp = scrollBuilder.execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {

        System.out.println(hit.getId());

        /* if (!hit.getId().equals(
            "https://en.wikipedia.org/wiki/Category:WikiProject_United_States_articles")) {
          continue;
        }*/
        Map<String, Object> metadata = hit.getSource();

        String url = (String) metadata.get("url");
        String host = (String) metadata.get("host");
        String content = (String) metadata.get("content");
        String title = (String) metadata.get("title");

        metadata.put("fileType", "webpage");
        // String organization = this.getOrganization(host);
        // metadata.put("organization", organization);

        JsonObject content_phrase = extrctor.NounPhraseExtractor(es,
            this.newIndex, content);
        JsonObject title_phrase = extrctor.NounPhraseExtractor(es,
            this.newIndex, title);
        String chunkerKeywords = extrctor.keyPhraseExtractor(title_phrase,
            content_phrase);
        metadata.put("content_chunker_keywords", chunkerKeywords);

        String keywords = extrctor.keyPhraseExtractor(title, content);
        metadata.put("content_orignal_Keywords", keywords);

        // metadata.put("content", "");
        // System.out.println("orignal keyword:" + keywords);
        // System.out.println("chunker content:" + chunkerKeywords);
        // System.out.println("************");
        //

        /*try {
        
          String content_phrases_str = content_phrase.get("phrase").toString();
          String content_phrases_pos = content_phrase.get("phrasePos")
              .toString();
        
          String title_phrases_str = title_phrase.get("phrase").toString();
          String title_phrases_pos = title_phrase.get("phrasePos").toString();
        
          System.out.println(content_phrases_str);
          System.out.println(content_phrases_pos);
          System.out.println(title_phrases_str);
          System.out.println(title_phrases_pos);
        
          metadata.put("content_phrases", content_phrases_str);
          metadata.put("title_phrases", title_phrases_str);
        
          metadata.put("content_phrases_pos", content_phrases_pos);
          metadata.put("title_phrases_pos", title_phrases_pos);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }*/

        IndexRequest ir = new IndexRequest(newIndex, newType).source(metadata);
        bulkProcessor.add(ir);

        /* UpdateRequest ur = new UpdateRequest(index, type, hit.getId())
            .doc(jsonBuilder().startObject()
                .field("content_chunker_keywords", chunkerKeywords)
                .field("content_orignal_Keywords", keywords).endObject());
        
        System.out.println(ur.toString());
        bulkProcessor.add(ur);*/
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return;
  }

  private String getOrganization(String host) {

    String organization = "";
    if (organizationMap.containsKey(host)) {
      organization = organizationMap.get(host);
    }

    String domain = "";
    String[] levels = host.split("\\.");
    int num = levels.length;
    if (num <= 2) {
      domain = host;
    } else {
      for (int i = num - 2; i < num; i++) {
        domain += levels[i] + ".";
      }
      domain = domain.substring(0, domain.length() - 1);
    }

    organization = getOrganizationFromDomain(domain);

    // System.out.println(domain);
    organizationMap.put(host, organization);

    return organization;
  }

  String[] vocabularies1 = new String[] { "Asteroid class",
      "Atmospheric breakup" };

  String[] vocabularies = new String[] { "101955 Bennu", "1999 RQ36",
      "3-D Visualization", "Acid Trauma", "Airburst modeling", "Approach",
      "Arecibo Observatory", "Asteroid class", "Atmospheric breakup",
      "Bistatic Doppler effect", "Blast and Thermal Propagation", "Bolides",
      "Casualty Sensitivity", "Chelyabinsk", "Chemical composition",
      "Chondrite", "Command", "Communications segment", "Contact",
      "Data centralization", "Data flow", "Data management", "Data mining",
      "Data sharing", "Decision Support Analysis", "Deep space station",
      "Design reference Asteroids", "Encounter", "Encounter mode",
      "Energy deposition", "Ephemeris", "Execution Connectivity",
      "Final Approach", "Final Terminal", "Fireball breakup",
      "Forward commands", "Geometric albedo", "Ground Segment",
      "High Fidelity Simulations", "Hyperspectral Imaging",
      "IAU Minor Planet Center", "Impact Disaster Planning Advisory Group",
      "Impact experiments", "Impactor", "Infrasonic Measurement",
      "International Asteroid Warning Network",
      "International Astronomical Union",
      "International Characterization Capability", "Ka-band system",
      "Kinetic impactor", "Knowledge integration", "Knowledge Reasoning Models",
      "Land, Water, and Atmospheric NEO Impact Effects", "Launch",
      "Launch segment", "Light Curve Database", "Luminous efficiency",
      "Material Equations of state", "Meteorite samples", "Meteorite Sampling",
      "Miss", "Mitigation", "Mitigation Communication",
      "Mitigation Data Assessment", "Mitigation Data Analysis",
      "Mitigation Data Collection", "Mitigation decision options",
      "Mitigation deployment options", "Mitigation determination options",
      "Mitigation execution", "NASA Ames Research Center",
      "NASA Goddard Space Flight Center", "NASA Jet Propulsion Laboratory",
      "NASA Jet Propulsion Laboratory Sentry Database",
      "NASA Planetary Defense Coordination Office", "NED Deflection",
      "NED Disruption", "NEO Ablation", "NEO Impact Ejecta",
      "NEO Impact hazards", "NEO Impact probability", "NEO Impact Tsunami",
      "NEO Impact velocity", "NEO Mitigation framework", "NEO Orbit Family",
      "NEO Porosity", "NEO Regolith", "NEO Rotational State",
      "NEO Spectral Taxonomic Class", "NEO-Earth Impact Risk",
      "NEO-Earth Impact Scenario Development", "NEOWISE", "NNSA Laboratories",
      "Non-projectile mitigation methods", "Nuclear impactor",
      "Object assessment", "Object characterization", "Orbit Determination",
      "Orbit Estimation", "Orbital debris tracking", "OSIRIS-REx",
      "Perturbation Climatology", "Petascale supercomputing",
      "Physics-based modeling", "Planetary Defense Information Architecture",
      " PDIA", "Planetary Defense Policy Development",
      "Planetary Defense Strategies", "Post Encounter",
      "Post Encounter Termination", "Post Launch Check Out", "Pre-launch",
      "Precursor mission", "Public/citizen engagement",
      "Quantitative Risk Metrics", "Radial velocity", "Radiative transport",
      "Radio telescopes", "Response Time", "situational awareness",
      "Risk Analysis", "Safe mode", "Secondary Effects", "Seismic shaking",
      "Space Segment", "Spectrophotometry", "Sustainability",
      "Thermal Emission Spectrometer", "Thermal Radiation", "Tracer analysis",
      "Trajectory analysis", "Transit", "Variational Analysis",
      "Vulnerability Analysis" };

  private String extractKeyWordFromGoldenText() {

    BoolQueryBuilder bq = this.createVocabularyQuery();
    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(newIndex)
        .setTypes(newType).setScroll(new TimeValue(60000)).setQuery(bq)
        .setSize(100).setExplain(true);

    SearchResponse scrollResp = scrollBuilder.execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();

        List<String> goldenKeywordList = new ArrayList<String>();
        Explanation totalexplain = hit.getExplanation();
        Explanation[] all_explains = totalexplain.getDetails();
        for (Explanation explain : all_explains) {
          Explanation[] nested_level1_explains = explain.getDetails();
          if (nested_level1_explains != null
              && nested_level1_explains.length > 0) {
            Explanation[] subexplains = nested_level1_explains[0].getDetails();
            if (subexplains != null && subexplains.length > 0) {
              Explanation nestedexplain = subexplains[0];
              String description = nestedexplain.getDescription();
              description = description.replaceAll("weight\\(content\\:", "");
              int pos = description.indexOf(" in ");
              String keyword = description.substring(0, pos);
              if (keyword.startsWith("\"")) {
                keyword = keyword.substring(1, keyword.length() - 2);
              }

              goldenKeywordList.add(keyword);
            }
          }
        }

        List<String> deduped = goldenKeywordList.stream().distinct()
            .collect(Collectors.toList());

        String goldKeywords = String.join(",", deduped);
        System.out.println(goldKeywords);

        UpdateRequest ur = null;
        try {
          ur = new UpdateRequest(index, type, hit.getId()).doc(jsonBuilder()
              .startObject().field("gold_keywords", goldKeywords).endObject());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        bulkProcessor.add(ur);
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return "";
  }

  public BoolQueryBuilder createVocabularyQuery() {

    BoolQueryBuilder qb = new BoolQueryBuilder();
    String fieldsList[] = { "Title", "content" };

    int length = vocabularies.length;
    for (int i = 0; i < length; i++) {
      String vob = vocabularies[i].toLowerCase();
      qb.should(QueryBuilders.multiMatchQuery(vob, fieldsList)
          .type(MultiMatchQueryBuilder.Type.PHRASE).tieBreaker((float) 0.5));
    }

    return qb;
  }

  private String getOrganizationFromDomain(String domain) {
    if (domain == null || domain.isEmpty()) {
      return "";
    }
    String title = "";
    try {
      title = TitleExtractor.getPageTitle("http://" + domain);
      if (title == null || title.isEmpty()) {
        title = TitleExtractor.getPageTitle("https://" + domain);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if (title == null) {
      title = "";
    } else {
      if (title.equals(
          "NASA Jet Propulsion Laboratory (JPL) - Space Mission and Science News, Videos and Images")) {
        title = "NASA Jet Propulsion Laboratory (JPL)";
      }
      title = title.trim();
      if (title.equals("undefined")) {
        title = "";
      }
    }

    return title;
  }

  public void extractKeyPhaseByTFIDF(int keyphraseNum) {

    // get total doc number
    SearchRequestBuilder countSrBuilder = es.client.prepareSearch(newIndex)
        .setTypes(newType).setQuery(QueryBuilders.matchAllQuery()).setSize(0);
    SearchResponse countSr = countSrBuilder.execute().actionGet();
    int docCount = (int) countSr.getHits().getTotalHits();

    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(newIndex)
        .setTypes(newType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        String id = (String) metadata.get("_id");
        String phrases = (String) metadata.get("phrases");
        String content = (String) metadata.get("content");

        HashMap<String, Double> phrase_tfidf = new HashMap<String, Double>();
        String[] chunks = phrases.split(",");
        int num = chunks.length;

        HashMap<String, Integer> termFrequencies = new LinkedHashMap<>();
        for (int i = 0; i < chunks.length; i++) {
          String phrase = chunks[i].toLowerCase();
          if (termFrequencies.containsKey(phrase)) {
            termFrequencies.put(phrase, termFrequencies.get(phrase) + 1);
          } else {
            termFrequencies.put(phrase, 1);
          }
        }

        for (String phrase : termFrequencies.keySet()) {
          double occurance = (double) termFrequencies.get(phrase);
          double tf = occurance / (double) num;
          double tfidf = getTFIDF(tf, docCount, phrase);
          phrase_tfidf.put(phrase, tfidf);
        }

        // System.out.println(phrase_tfidf);
        Map<String, Double> sortedMap = sortMapByValue(phrase_tfidf);

        // System.out.println(sortedMap);
        int count = 0;
        String keyphrase = "";
        for (Entry<String, Double> entry : sortedMap.entrySet()) {
          if (count < keyphraseNum) {
            keyphrase += entry.getKey() + ", ";
          }
          count++;
        }

        System.out.println(keyphrase);

      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return;
  }

  public void calculateNounPhaseTFIDF(String field, int keyphraseNum)
      throws IOException {

    // get total doc number
    SearchRequestBuilder countSrBuilder = es.client.prepareSearch(newIndex)
        .setTypes(newType).setQuery(QueryBuilders.matchAllQuery()).setSize(0);
    SearchResponse countSr = countSrBuilder.execute().actionGet();
    int docCount = (int) countSr.getHits().getTotalHits();

    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(newIndex)
        .setTypes(newType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100);
    SearchResponse scrollResp = scrollBuilder.execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        String id = (String) metadata.get("_id");
        String phrases = (String) metadata.get(field);
        HashMap<String, Double> phrase_tfidf = new HashMap<String, Double>();
        String[] chunks = phrases.split(",");
        int num = chunks.length;

        HashMap<String, Integer> termFrequencies = new LinkedHashMap<>();
        for (int i = 0; i < chunks.length; i++) {
          String phrase = chunks[i].toLowerCase();
          if (termFrequencies.containsKey(phrase)) {
            termFrequencies.put(phrase, termFrequencies.get(phrase) + 1);
          } else {
            termFrequencies.put(phrase, 1);
          }
        }

        for (String phrase : termFrequencies.keySet()) {
          double occurance = (double) termFrequencies.get(phrase);
          double tf = occurance / (double) num;
          double tfidf = getTFIDF(tf, docCount, phrase);
          phrase_tfidf.put(phrase, tfidf);
        }

        // System.out.println(phrase_tfidf);
        Map<String, Double> sortedMap = sortMapByValue(phrase_tfidf);
        Map<String, Double> candidatedMap = new LinkedHashMap<String, Double>();

        // System.out.println(sortedMap);
        int count = 0;
        String keyphrase = "";
        for (Entry<String, Double> entry : sortedMap.entrySet()) {
          if (count < keyphraseNum) {
            keyphrase += entry.getKey() + ", ";
            candidatedMap.put(entry.getKey(), entry.getValue());
          }
          count++;
        }

        String candidateJson = new Gson().toJson(candidatedMap);

        UpdateRequest ur = new UpdateRequest(newIndex, newType, hit.getId())
            .doc(jsonBuilder().startObject()
                .field(field + "_tfidf", candidateJson).endObject());
        bulkProcessor.add(ur);
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return;
  }

  public double getTFIDF(double tf, int numdoc, String phrase) {
    double tf_root = Math.sqrt(tf);

    SearchRequestBuilder srbuilder = es.client.prepareSearch(this.newIndex)
        .setTypes(newType)
        .setQuery(QueryBuilders.termQuery("phrases", phrase.toLowerCase()))
        .setSize(0);

    SearchResponse sr = srbuilder.execute().actionGet();

    int docFreq = (int) sr.getHits().getTotalHits();
    double tmp = (double) numdoc / (docFreq + 1);
    double idf = 1 + Math.log10(tmp);
    double tfidf = tf_root * idf;

    return tfidf;
  }

  /**
   * Method of sorting a map by value
   * 
   * @param passedMap
   *          input map
   * @return sorted map
   */
  public Map<String, Double> sortMapByValue(Map<String, Double> passedMap) {
    List<String> mapKeys = new ArrayList<>(passedMap.keySet());
    List<Double> mapValues = new ArrayList<>(passedMap.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());

    LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();

    Iterator<Double> valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
      Object val = valueIt.next();
      Iterator<String> keyIt = mapKeys.iterator();

      while (keyIt.hasNext()) {
        Object key = keyIt.next();
        String comp1 = passedMap.get(key).toString();
        String comp2 = val.toString();

        if (comp1.equals(comp2)) {
          passedMap.remove(key);
          mapKeys.remove(key);
          sortedMap.put((String) key, (Double) val);
          break;
        }
      }
    }
    return sortedMap;
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    ReindexNutchResult nutchIndexer = new ReindexNutchResult();
    try {
      nutchIndexer.extractKeyWordFromGoldenText();
      nutchIndexer.es.closeES();
      // nutchIndexer.calculateNounPhaseTFIDF("content_phrases", 5);
      // nutchIndexer.calculateNounPhaseTFIDF("title_phrases", 2);
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }
}
