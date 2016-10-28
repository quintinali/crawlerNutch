package pd.crawler4s.crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import pd.crawler4s.driver.ESdriver;

public class MyCrawler extends WebCrawler implements Serializable {
  private final static Pattern FILTERS = Pattern
      .compile(".*(\\.(css|js|gif|jpg" + "|png|mp3|mp3|zip|gz))$");
  private final String index = "pdcrawler";
  private final String type = "crawler4j";
  public static ESdriver esd = new ESdriver();
  public static BulkProcessor bulkProcessor = BulkProcessor
      .builder(esd.client, new BulkProcessor.Listener() {
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

  private Map<String, String> organizationMap = new HashMap<String, String>();

  @Override
  public boolean shouldVisit(Page referringPage, WebURL url) {
    String href = url.getURL().toLowerCase();
    return !FILTERS.matcher(href).matches()
        && !href.contains("http://ssd.jpl.nasa.gov/sbdb.cgi?sstr=");
  }

  /**
   * This function is called when a page is fetched and ready to be processed by
   * your program.
   */
  @Override
  public void visit(Page page) {
    String url = page.getWebURL().getURL();
    if (page.getParseData() instanceof HtmlParseData) {
      HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
      String text = htmlParseData.getText();
      text = text.replaceAll("[^\\S\\r\\n]+", " ").replaceAll("\\n+", " ")
          .replaceAll("\\s+", " ");
      IndexRequest ir;

      // get organization
      String organization = this.getOrganization(url);

      try {
        ir = new IndexRequest(index, type).source(jsonBuilder().startObject()
            .field("URL", url).field("Title", htmlParseData.getTitle())
            .field("Time", new Date()).field("content", text)
            .field("fileType", "webpage").field("organization", organization)
            .endObject());
        bulkProcessor.add(ir);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private String getOrganization(String url) {
    url = url.toLowerCase();
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
      return "";
    }

    int prefixIndex = url.indexOf("//");
    int domainIndex = url.indexOf("/", prefixIndex + 2);
    String predix = url.substring(0, prefixIndex - 1);
    String domain = url.substring(prefixIndex + 2, domainIndex);
    String[] skipList = { "global.jaxa.jp", "edu.jaxa.jp", "neo.ssa.esa.int" };
    if (!Arrays.asList(skipList).contains(domain)) {
      String[] levels = domain.split("\\.");
      int num = levels.length;
      if (num < 1) {
        return "";
      }

      if (!levels[0].equals("www") && num > 1) {
        String parentdomain = "";
        for (int i = 1; i < num; i++) {
          parentdomain += levels[i] + ".";
        }
        domain = parentdomain.substring(0, parentdomain.length() - 1);
      }
    }

    String domainUrl = predix + "://" + domain;

    String organization = "";
    if (organizationMap.containsKey(domainUrl)) {
      organization = organizationMap.get(domainUrl);
    } else {

      String title = "";
      try {
        title = TitleExtractor.getPageTitle(domainUrl);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      organization = title;
      /*if(organization.indexOf("-") > -1) {
        organization = organization.substring(0, organization.indexOf("-"));
      }
      if(organization.indexOf("|") > -1) {
        organization = organization.substring(0, organization.indexOf("|"));
      }*/

      if (organization.equals(
          "NASA Jet Propulsion Laboratory (JPL) - Space Mission and Science News, Videos and Images")) {
        organization = "NASA Jet Propulsion Laboratory (JPL)";
      }
      organization = organization.trim();
      if (organization.equals("undefined")) {
        organization = "";
      }
      organizationMap.put(domainUrl, organization);
    }

    // System.out.println(url);
    // System.out.println(domainUrl);
    // System.out.println("organization:" + organization);

    return organization;

  }

  // for import cvs
  /* public void loadFromCSV(String inputFileName, String outputFile)
      throws Exception {
  
    SparkConf conf = new SparkConf().setAppName("Testing")
        .setMaster("local[2]");
    JavaSparkContext sc = new JavaSparkContext(conf);
  
    JavaRDD<String> importRDD = sc.textFile(inputFileName);
  
    JavaPairRDD<String, Integer> domainRDD = importRDD
        .mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String arg0) throws Exception {
            // TODO Auto-generated method stub
            String line = arg0;
            String url = getURL(line);
            String domain = "";
            if (url.startsWith("http")) {
              domain = getDomainName(url);
            }
  
            return new Tuple2<String, Integer>(domain, 1);
          }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          private static final long serialVersionUID = 1L;
  
          @Override
          public Integer call(Integer arg0, Integer arg1) throws Exception {
            // TODO Auto-generated method stub
            return arg0 + arg1;
          }
        }).sortByKey(false);
  
    System.out.println(domainRDD.collect());
  
    domainRDD.keys().coalesce(1).saveAsTextFile(outputFile);
  }
  
  public String getURL(String line) throws URISyntaxException {
  
    String[] parts = line.split(",");
    int length = parts.length;
    String url = "";
    for (int i = length - 1; i > 0; i--) {
      url = parts[i];
      if (url.startsWith("http://") || url.startsWith("https://")) {
        break;
      }
    }
  
    return url;
  }
  
  public String getDomainName(String url) {
    if (url.length() > 400) {
      url = url.substring(0, 400);
    }
    URI uri;
    try {
      uri = new URI(url);
      String domain = uri.getHost();
      if (domain == null) {
        return "error";
      }
      return domain.startsWith("www.") ? domain.substring(4) : domain;
    } catch (URISyntaxException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return "error";
  
  }*/

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    String url = "http://global.jaxa.jp/press/2014/11/20141125_daichi2.html";
    MyCrawler crawler = new MyCrawler();
    String org = crawler.getOrganization(url);
    System.out.println("org:" + org);
  }

  public MyCrawler() {

  }

}
