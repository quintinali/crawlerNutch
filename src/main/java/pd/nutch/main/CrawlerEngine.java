package pd.nutch.main;

import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import pd.nutch.driver.ESDriver;
import pd.nutch.driver.SparkDriver;

public class CrawlerEngine {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlerEngine.class);
  private Properties props = new Properties();
  private ESDriver es = null;
  private SparkDriver spark = null;
  private static final String LOG_INGEST = "logIngest";

  public Properties loadConfig() {
    SAXBuilder saxBuilder = new SAXBuilder();
    InputStream configStream = CrawlerEngine.class.getClassLoader().getResourceAsStream("config.xml");

    Document document;
    try {
      document = saxBuilder.build(configStream);
      Element rootNode = document.getRootElement();
      List<Element> paraList = rootNode.getChildren("para");

      for (int i = 0; i < paraList.size(); i++) {
        Element paraNode = paraList.get(i);
        props.put(paraNode.getAttributeValue("name"), paraNode.getTextTrim());
      }
    } catch (JDOMException | IOException e) {
      LOG.error("Exception whilst retreiving or processing XML contained within 'config.xml'!", e);
    }
    return getConfig();
  }

  public Properties getConfig() {
    return props;
  }

  public ESDriver getES() {
    return this.es;
  }

  public void end() {
    es.close();
  }

  public void setES(ESDriver es) {
    this.es = es;
  }

  public void logIngest() {

  }

  /**
   * Main program invocation. Accepts one argument denoting location (on disk)
   * to a log file which is to be ingested. Help will be provided if invoked
   * with incorrect parameters.
   * 
   * @param args
   *          {@link java.lang.String} array contaning correct parameters.
   */
  public static void main(String[] args) {
    System.out.println(0);
    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");

    // import raw web log into Elasticsearch
    Option logIngestOpt = new Option("l", LOG_INGEST, false, "begin log ingest without any processing only");

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(logIngestOpt);

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      String processingType = null;

      processingType = LOG_INGEST;

      System.out.println(1);
      CrawlerEngine me = new CrawlerEngine();
      me.loadConfig();
      System.out.println(2);
      me.es = new ESDriver(me.getConfig());
      System.out.println(3);
      me.spark = new SparkDriver(me.getConfig());

      System.out.println(4);
      switch (processingType) {
      case LOG_INGEST:
        me.es.deleteType("new_nutch", "doc");
        break;
      default:
        break;
      }
      me.end();
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("MudrodEngine: 'logDir' argument is mandatory. " + "User must also provide either 'logIngest' or 'fullIngest'.", options, true);
      LOG.error("MudrodEngine: 'logDir' argument is mandatory. " + "User must also provide either 'logIngest' or 'fullIngest'.", e);
      return;
    }
  }
}
