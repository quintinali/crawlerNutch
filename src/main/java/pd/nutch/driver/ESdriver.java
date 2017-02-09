package pd.nutch.driver;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ESdriver {
  String cluster = "elasticsearch";
  String index = "nutchseedindex";
  String crawlerType = "doc";
  final Integer MAX_CHAR = 500;

  Node node;

  public Client client;

  public BulkProcessor bulkProcessor = null;

  public ESdriver() {
    Settings settings = System.getProperty("file.separator").equals("/")
        ? ImmutableSettings.settingsBuilder().put("http.enabled", "false")
            .put("transport.tcp.port", "9300-9400")
            .put("discovery.zen.ping.multicast.enabled", "false")
            .put("discovery.zen.ping.unicast.hosts", "localhost").build()
        : ImmutableSettings.settingsBuilder().put("http.enabled", false)
            .build();

    node = nodeBuilder().client(true).settings(settings).clusterName(cluster)
        .node();
    client = node.client();

    try {
      putMapping(index);
    } catch (ElasticsearchException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void RefreshIndex() {
    node.client().admin().indices().prepareRefresh().execute().actionGet();
  }

  public void putMapping(String index)
      throws ElasticsearchException, IOException {
    boolean exists = client.admin().indices().prepareExists(index).execute()
        .actionGet().isExists();
    if (exists) {
      return;
    }

    client.admin().indices().prepareCreate(index).setSettings(ImmutableSettings
        .settingsBuilder()
        .loadFromSource(jsonBuilder().startObject().startObject("analysis")
            .startObject("filter").startObject("cody_stop")
            .field("type", "stop").field("stopwords", "_english_").endObject()
            .startObject("cody_stemmer").field("type", "stemmer")
            .field("stopwords", "light_english").endObject().endObject()
            .startObject("analyzer").startObject("csv").field("type", "pattern")
            .field("pattern", ",").endObject().startObject("cody")
            .field("tokenizer", "standard")
            .field("filter", new String[] { "lowercase", "cody_stop" })
            .endObject().endObject().endObject().endObject().string()))
        .execute().actionGet();

    XContentBuilder pageMapping = jsonBuilder().startObject()
        .startObject("_default_").startObject("properties")
        .startObject("fileType").field("type", "string")
        .field("index", "not_analyzed").endObject()
        .startObject("content_phrases_pos").field("type", "string")
        .field("index", "not_analyzed").endObject()
        .startObject("title_phrases_pos").field("type", "string")
        .field("index", "not_analyzed").endObject().startObject("url")
        .field("type", "string").field("index", "not_analyzed").endObject()
        .startObject("content_phrases").field("type", "string")
        .field("index_analyzer", "csv").endObject().startObject("title_phrases")
        .field("type", "string").field("index_analyzer", "csv").endObject()
        .startObject("keywords").field("type", "string")
        .field("index_analyzer", "csv").endObject().endObject().endObject()
        .endObject();

    client.admin().indices().preparePutMapping(index).setType("_default_")
        .setSource(pageMapping).execute().actionGet();

  }

  public BulkProcessor createBulkProcesser() {
    bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
      public void beforeBulk(long executionId, BulkRequest request) {
        /*System.out.println("New request!");*/}

      public void afterBulk(long executionId, BulkRequest request,
          BulkResponse response) {
        /*System.out.println("Well done!");*/}

      public void afterBulk(long executionId, BulkRequest request,
          Throwable failure) {
        System.out.println("Bulk fails!");
        throw new RuntimeException(
            "Caught exception in bulk: " + request + ", failure: " + failure,
            failure);
      }
    }).setBulkActions(50).setBulkSize(new ByteSizeValue(100, ByteSizeUnit.MB))
        .setConcurrentRequests(4).build();
    return bulkProcessor;
  }

  public void destroyBulkProcessor() {
    try {
      bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
      bulkProcessor = null;
      node.client().admin().indices().prepareRefresh().execute().actionGet();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public boolean checkItemExist(String type, String keyName, String value) {
    CountResponse count = client.prepareCount(index).setTypes(type)
        .setQuery(QueryBuilders.termQuery(keyName, value)).execute()
        .actionGet();

    if (count.getCount() == 0) {
      return true;
    } else {
      return false;
    }
  }

  public boolean checkTypeExist(String index, String type) {
    GetMappingsResponse res;
    try {
      res = client.admin().indices()
          .getMappings(new GetMappingsRequest().indices(index)).get();
      ImmutableOpenMap<String, MappingMetaData> mapping = res.mappings()
          .get(index);
      for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
        if (c.key.equals(type)) {
          return true;
        }
      }
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  public String searchByQuery(String query, int from, int limit) {
    boolean exists = node.client().admin().indices().prepareExists(index)
        .execute().actionGet().isExists();
    if (!exists) {
      return null;
    }

    // query = "jaxa";
    QueryBuilder qb = null;
    if (query.equals("")) {
      qb = QueryBuilders.matchAllQuery();
    } else {
      qb = QueryBuilders.queryStringQuery(query);
    }

    if (limit <= 0) {
      limit = 200;
    }

    if (from <= 0) {
      from = 0;
    }

    int docCount = getDocCount(index, qb, crawlerType);

    SearchResponse scrollResp = null;

    SearchRequestBuilder sbuilder = client.prepareSearch(index)
        .setTypes(crawlerType).addHighlightedField("title")
        .addHighlightedField("content").setScroll(new TimeValue(60000))
        .addSort("tstamp", SortOrder.ASC).setQuery(qb).setFrom(from)
        .setSize(limit).setHighlighterPreTags("<b>")
        .setHighlighterPostTags("</b>").setHighlighterFragmentSize(500)
        .setHighlighterNumOfFragments(1);

    scrollResp = sbuilder.execute().actionGet();

    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<JsonObject>();

    for (SearchHit hit : scrollResp.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String fileType = (String) result.get("fileType");
      String Time = (String) result.get("tstamp");
      String Content = (String) result.get("content");
      String chunkerKeyword = (String) result.get("chunker_keywords");
      String keywords = (String) result.get("original_keywords");
      String goldKeywords = (String) result.get("gold_keywords");

      String summary = Content;

      String Title, URL = null;
      Title = (String) result.get("title");
      URL = (String) result.get("url");

      /*if (fileType != null && fileType.equals("webpage")) {
        Title = (String) result.get("title");
        URL = (String) result.get("url");
      } else {
        Title = (String) result.get("fullName");
        URL = "";
      }*/

      String orgnization = (String) result.get("organization");

      // get highlight content
      Map<String, HighlightField> highlights = hit.highlightFields();
      for (String s : highlights.keySet()) {

        HighlightField highlight = highlights.get(s);
        Text[] texts = highlight.getFragments();

        if (s.equals("title")) {
          Title = texts[0].toString();
        }

        if (s.equals("content")) {
          summary = texts[0].toString();
        }
      }

      JsonObject file = new JsonObject();
      file.addProperty("Title", Title);
      file.addProperty("Organization", orgnization);
      file.addProperty("URL", URL);
      file.addProperty("Content", Content);
      file.addProperty("chunkerKeyword", chunkerKeyword);
      file.addProperty("keywords", keywords);
      file.addProperty("goldKeywords", goldKeywords);
      file.addProperty("summary", summary);
      fileList.add(file);
    }

    JsonElement fileList_Element = gson.toJsonTree(fileList);
    JsonObject PDResults = new JsonObject();

    PDResults.add("SearchResults", fileList_Element);
    PDResults.addProperty("ResultCount", docCount);

    return PDResults.toString();
  }

  public int getDocCount(String index, QueryBuilder filterSearch,
      String... type) {
    SearchRequestBuilder countSrBuilder = node.client().prepareSearch(index)
        .setTypes(type).setQuery(filterSearch).setSize(0);
    SearchResponse countSr = countSrBuilder.execute().actionGet();
    int docCount = (int) countSr.getHits().getTotalHits();
    return docCount;
  }

  public List<String> autoComplete(String chars) {
    boolean exists = node.client().admin().indices().prepareExists(index)
        .execute().actionGet().isExists();
    if (!exists) {
      return null;
    }

    List<String> SuggestList = new ArrayList<String>();

    CompletionSuggestionFuzzyBuilder suggestionsBuilder = new CompletionSuggestionFuzzyBuilder(
        "completeMe");
    suggestionsBuilder.text(chars);
    suggestionsBuilder.size(10);
    suggestionsBuilder.field("name_suggest");
    suggestionsBuilder.setFuzziness(Fuzziness.fromEdits(2));

    SuggestRequestBuilder suggestRequestBuilder = client.prepareSuggest(index)
        .addSuggestion(suggestionsBuilder);

    SuggestResponse suggestResponse = suggestRequestBuilder.execute()
        .actionGet();

    Iterator<? extends Suggest.Suggestion.Entry.Option> iterator = suggestResponse
        .getSuggest().getSuggestion("completeMe").iterator().next().getOptions()
        .iterator();

    while (iterator.hasNext()) {
      Suggest.Suggestion.Entry.Option next = iterator.next();
      SuggestList.add(next.getText().string());
    }
    return SuggestList;

  }

  public String customAnalyzing(String indexName, String str)
      throws InterruptedException, ExecutionException {
    return this.customAnalyzing(indexName, "cody", str);
  }

  public String customAnalyzing(String indexName, String analyzer, String str)
      throws InterruptedException, ExecutionException {
    String[] strList = str.toLowerCase().split(",");
    for (int i = 0; i < strList.length; i++) {
      String tmp = "";
      AnalyzeResponse r = client.admin().indices().prepareAnalyze(strList[i])
          .setIndex(indexName).setAnalyzer(analyzer).execute().get();
      for (AnalyzeToken token : r.getTokens()) {
        tmp += token.getTerm() + " ";
      }
      strList[i] = tmp.trim();
    }
    return String.join(",", strList);
  }

  public void closeES() throws InterruptedException {
    // bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
    node.close();
  }

}
