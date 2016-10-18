package pd.crawler4s.driver;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ESdriver {
  String cluster = "elasticsearch";
  String index = "pd";
  String crawlerType = "crawler4j";
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

    putMapping(index);
  }

  public void RefreshIndex() {
    node.client().admin().indices().prepareRefresh().execute().actionGet();
  }

  public void putMapping(String index) {
    boolean exists = client.admin().indices().prepareExists(index).execute()
        .actionGet().isExists();
    if (exists) {
      return;
    }

    String settings_json = "{\r\n    \"analysis\": {\r\n      \"filter\": {\r\n        \"cody_stop\": {\r\n          \"type\":        \"stop\",\r\n          \"stopwords\": \"_english_\"  \r\n        },\r\n        \"cody_stemmer\": {\r\n          \"type\":       \"stemmer\",\r\n          \"language\":   \"light_english\" \r\n        }       \r\n      },\r\n      \"analyzer\": {\r\n        \"cody\": {\r\n          \"tokenizer\": \"standard\",\r\n          \"filter\": [ \r\n            \"lowercase\",\r\n            \"cody_stop\",\r\n            \"cody_stemmer\"\r\n          ]\r\n        }\r\n      }\r\n    }\r\n  }";
    String mapping_json = "{\r\n      \"_default_\": {\r\n         \"properties\": {            \r\n            \"fullName\": {\r\n                \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"shortName\": {\r\n                \"type\" : \"string\", \r\n                \"analyzer\": \"cody\"\r\n            },\r\n            \"name_suggest\" : {\r\n                \"type\" :  \"completion\"\r\n            },\r\n            \"content\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            },\r\n           \"time\": {\r\n               \"type\":   \"date\",\r\n               \"format\": \"yyyy-MM-dd\"\r\n            },\r\n            \"metaContent\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"cody\"\r\n            }\r\n         }\r\n      }\r\n   }      \r\n";
    client.admin().indices().prepareCreate(index)
        .setSettings(
            ImmutableSettings.settingsBuilder().loadFromSource(settings_json))
        .execute().actionGet();
    client.admin().indices().preparePutMapping(index).setType("_default_")
        .setSource(mapping_json).execute().actionGet();
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
    }).setBulkActions(1000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
        .setConcurrentRequests(1).build();
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

  /*public String searchByQuery(String query, String filter,
      String filter_field) {
    boolean exists = node.client().admin().indices().prepareExists(index)
        .execute().actionGet().isExists();
    if (!exists) {
      return null;
    }
  
    QueryBuilder qb = null;
    if (filter.equals("")) {
      qb = QueryBuilders.queryStringQuery(query);
    } else {
      FilterBuilder filter_search = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter(filter_field, filter));
      qb = QueryBuilders.filteredQuery(QueryBuilders.queryStringQuery(query),
          filter_search);
    }
    SearchResponse response = client.prepareSearch(index).setTypes(crawlerType)
        .setQuery(qb).setSize(20)
        .addAggregation(
            AggregationBuilders.terms("Types").field("fileType").size(0))
        .execute().actionGet();
  
    Terms Types = response.getAggregations().get("Types");
    List<JsonObject> TypeList = new ArrayList<JsonObject>();
    for (Terms.Bucket entry : Types.getBuckets()) {
      JsonObject Type = new JsonObject();
      Type.addProperty("Key", entry.getKey());
      Type.addProperty("Value", entry.getDocCount());
      TypeList.add(Type);
    }
  
    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<JsonObject>();
  
    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String fileType = (String) result.get("fileType");
      String Time = (String) result.get("Time");
      String Content = (String) result.get("content");
      String Title, URL = null;
      if (fileType.equals("webpage")) {
        Title = (String) result.get("Title");
        URL = (String) result.get("URL");
      } else {
        Title = (String) result.get("fullName");
        URL = "";
      }
  
      if (!Content.equals("")) {
        int maxLength = (Content.length() < MAX_CHAR) ? Content.length()
            : MAX_CHAR;
        Content = Content.trim().substring(0, maxLength - 1) + "...";
      }
  
      JsonObject file = new JsonObject();
      file.addProperty("Title", Title);
      file.addProperty("Time", Time);
      file.addProperty("Type", fileType);
      file.addProperty("URL", URL);
      file.addProperty("Content", Content);
      fileList.add(file);
  
    }
    JsonElement fileList_Element = gson.toJsonTree(fileList);
    JsonElement TypeList_Element = gson.toJsonTree(TypeList);
  
    JsonObject PDResults = new JsonObject();
    JsonObject FacetResults = new JsonObject();
    PDResults.add("SearchResults", fileList_Element);
  
    FacetResults.add("fileType", TypeList_Element);
  
    PDResults.add("FacetResults", FacetResults);
    return PDResults.toString();
  }*/

  public String searchByQuery(String query, String scrollId, int limit) {
    boolean exists = node.client().admin().indices().prepareExists(index)
        .execute().actionGet().isExists();
    if (!exists) {
      return null;
    }

    QueryBuilder qb = null;
    if (query.equals("")) {
      qb = QueryBuilders.matchAllQuery();
    } else {
      qb = QueryBuilders.queryStringQuery(query);
    }

    if (limit <= 0) {
      limit = 200;
    }

    int docCount = getDocCount(index, qb, crawlerType);

    SearchResponse scrollResp = null;
    if (scrollId.equals("")) {
      scrollResp = client.prepareSearch(index).setTypes(crawlerType)
          .setScroll(new TimeValue(60000)).addSort("Time", SortOrder.ASC)
          .setQuery(qb).setSize(limit).execute().actionGet();
    } else {

      scrollResp = client.prepareSearchScroll(scrollId)
          .setScroll(new TimeValue(60000)).execute().actionGet();
    }

    String newScrollId = scrollResp.getScrollId();

    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<JsonObject>();

    for (SearchHit hit : scrollResp.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String fileType = (String) result.get("fileType");
      String Time = (String) result.get("Time");
      String Content = (String) result.get("content");
      String Title, URL = null;
      if (fileType.equals("webpage")) {
        Title = (String) result.get("Title");
        URL = (String) result.get("URL");
      } else {
        Title = (String) result.get("fullName");
        URL = "";
      }

      String orgnization = (String) result.get("organization");

      /*if (!Content.equals("")) {
        int maxLength = (Content.length() < MAX_CHAR) ? Content.length()
            : MAX_CHAR;
        Content = Content.trim().substring(0, maxLength - 1) + "...";
      }*/

      JsonObject file = new JsonObject();
      file.addProperty("Title", Title);
      // file.addProperty("Time", Time);
      // file.addProperty("Type", fileType);
      file.addProperty("Organization", orgnization);
      file.addProperty("URL", URL);
      file.addProperty("Content", Content);
      fileList.add(file);

    }
    JsonElement fileList_Element = gson.toJsonTree(fileList);
    JsonObject PDResults = new JsonObject();

    PDResults.add("SearchResults", fileList_Element);
    PDResults.addProperty("ResultCount", docCount);
    PDResults.addProperty("ScrollID", newScrollId);

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

  public void closeES() throws InterruptedException {
    // bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
    node.close();
  }

}
