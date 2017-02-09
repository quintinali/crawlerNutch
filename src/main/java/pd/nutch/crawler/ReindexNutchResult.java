package pd.nutch.crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MissingFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.gson.JsonObject;

import pd.nutch.driver.ESdriver;

public class ReindexNutchResult implements Serializable {

  private final String index = "nutchseedindex";
  private final String type = "doc";

  private final String newIndex = "testindex";
  private final String newType = "testdoc";

  private Map<String, String> organizationMap = new HashMap<String, String>();

  public static ESdriver es = new ESdriver();

  public void reIndex()
      throws IOException, InterruptedException, ExecutionException {

    es.putMapping(this.newIndex);
    es.createBulkProcesser();

    MissingFilterBuilder filter = FilterBuilders
        .missingFilter("content_chunker_keywords");
    QueryBuilder type_query_search = QueryBuilders
        .filteredQuery(QueryBuilders.matchAllQuery(), filter);
    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(index)
        .setTypes(type).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100);

    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    OpenNLPPhraseExtractor extrctor = new OpenNLPPhraseExtractor(es);
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {

        System.out.println(hit.getId());

        /*if (!hit.getId().equals(
            "https://en.wikipedia.org/wiki/Category:WikiProject_United_States_articles")) {
          continue;
        }*/
        Map<String, Object> metadata = hit.getSource();

        String url = (String) metadata.get("url");
        String host = (String) metadata.get("host");
        String content = (String) metadata.get("content");
        String title = (String) metadata.get("title");
        // String organization = this.getOrganization(host);

        String keywords = extrctor.keyPhraseExtractor(es, title, content);

        JsonObject content_phrase = extrctor.NounPhraseExtractor(es, content);
        JsonObject title_phrase = extrctor.NounPhraseExtractor(es, title);
        String chunkerKeywords = extrctor.keyPhraseExtractor(es, title_phrase,
            content_phrase);

        metadata.put("fileType", "webpage");
        metadata.put("content_orignal_Keywords", keywords);
        metadata.put("content_chunker_keywords", chunkerKeywords);
        // metadata.put("organization", organization);

        UpdateRequest ur = new UpdateRequest(index, type, hit.getId())
            .doc(jsonBuilder().startObject().field("fileType", "webpage")
                .field("chunker_keywords", chunkerKeywords)
                .field("original_keywords", keywords).endObject());

        es.bulkProcessor.add(ur);
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();

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

  public void extractKeyWordFromGoldenText() {
    GoldKeywordExtractor extrator = new GoldKeywordExtractor();
    extrator.extractKeyWordFromGoldenText(es, index, type);
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    ReindexNutchResult nutchIndexer = new ReindexNutchResult();
    try {
      nutchIndexer.reIndex();
      nutchIndexer.es.closeES();
      // nutchIndexer.calculateNounPhaseTFIDF("content_phrases", 5);
      // nutchIndexer.calculateNounPhaseTFIDF("title_phrases", 2);
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }
}
