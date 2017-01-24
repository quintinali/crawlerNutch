package pd.crawler4s.crawler;

import java.util.Map;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import pd.crawler4s.driver.ESdriver;

public class ReindexCrawlerResult {

  private final String index = "pdcrawler";
  private final String type = "crawler4j";
  private final String newType = "filteredCrawler4j";
  public static ESdriver es = new ESdriver();
  String[] vocabularies = new String[] { "101955 Bennu (1999 RQ36)",
      "3-D Visualization", "Acid Trauma", "Airburst modeling", "Approach",
      "Arecibo Observatory", "Asteroid class", "Atmospheric breakup",
      "Bistatic Doppler effect", "Blast and Thermal Propagation", "Bolides",
      "Casualty Sensitivity", "Chelyabinsk", "Chemical composition",
      "Chondrite", "Command", "Communications segment", "Contact",
      "Data centralization", "Data flow", "Data management", "Data mining",
      "Data sharing", "Decision Support Analysis", "Deep space station",
      "Design reference Asteroids", "Encounter", "Encounter mode",
      "Energy deposition", "Ephemeris", "Execution Connectivity",
      "Final Approach / Terminal", "Fireball breakup", "Forward (commands)",
      "Geometric albedo", "Ground Segment", "High Fidelity Simulations",
      "Hyperspectral Imaging", "IAU Minor Planet Center",
      "Impact Disaster Planning Advisory Group", "Impact experiments",
      "Impactor", "Infrasonic Measurement",
      "International Asteroid Warning Network",
      "International Astronomical Union",
      "International Characterization Capability", "Ka-band system",
      "Kinetic impactor", "Knowledge integration", "Knowledge Reasoning Models",
      "Land, Water, and Atmospheric NEO Impact Effects", "Launch",
      "Launch segment", "Light Curve Database", "Luminous efficiency",
      "Material Equations of state", "Meteorite samples", "Meteorite Sampling",
      "Miss", "Mitigation", "Mitigation Communication",
      "Mitigation Data Assessment / Analysis", "Mitigation Data Collection",
      "Mitigation decision / deployment options",
      "Mitigation determination options", "Mitigation execution",
      "NASA Ames Research Center", "NASA Goddard Space Flight Center",
      "NASA Jet Propulsion Laboratory",
      "NASA Jet Propulsion Laboratory Sentry Database",
      "NASA Planetary Defense Coordination Office", "NED Deflection",
      "NED Disruption", "NEO Ablation", "NEO Impact Ejecta",
      "NEO Impact hazards", "NEO Impact probability", "NEO Impact Tsunami",
      "NEO Impact velocity", "NEO Mitigation framework", "NEO Orbit Family",
      "NEO Porosity", "NEO Regolith", "NEO Rotational State",
      "NEO Spectral Taxonomic Class", "NEO-Earth Impact Risk",
      "NEO-Earth Impact Scenario Development", "NEOWISE", "NNSA Laboratories",
      "Non-projectile mitigation methods", "Nuclear impactor",
      "Object assessment / characterization", "Orbit Determination/Estimation",
      "Orbital debris tracking", "OSIRIS-REx", "Perturbation Climatology",
      "Petascale supercomputing", "Physics-based modeling",
      "Planetary Defense Information Architecture (PDIA)",
      "Planetary Defense Policy Development", "Planetary Defense Strategies",
      "Post Encounter", "Post Encounter Termination", "Post Launch Check Out",
      "Pre-launch", "Precursor mission", "Public/citizen engagement",
      "Quantitative Risk Metrics", "Radial velocity", "Radiative transport",
      "Radio telescopes", "Response Time (after warning time)",
      "Return (situational awareness)", "Risk Analysis", "Safe mode",
      "Secondary Effects", "Seismic shaking", "Space Segment",
      "Spectrophotometry", "Sustainability", "Thermal Emission Spectrometer",
      "Thermal Radiation", "Tracer analysis", "Trajectory analysis", "Transit",
      "Variational Analysis", "Vulnerability Analysis" };

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

  public int reIndex() {

    BoolQueryBuilder bq = this.createVocabularyQuery();
    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(index)
        .setTypes(type).setScroll(new TimeValue(60000)).setQuery(bq)
        .setSize(100);

    System.out.println(scrollBuilder.toString());

    SearchResponse scrollResp = scrollBuilder.execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        IndexRequest ir = new IndexRequest(index, newType).source(metadata);
        bulkProcessor.add(ir);
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return 1;
  }

  public BoolQueryBuilder createVocabularyQuery() {

    BoolQueryBuilder qb = new BoolQueryBuilder();
    String fieldsList[] = { "Title", "content", "organization" };

    int length = vocabularies.length;
    for (int i = 0; i < length; i++) {
      String vob = vocabularies[i].toLowerCase();
      qb.should(QueryBuilders.multiMatchQuery(vob, fieldsList)
          .type(MultiMatchQueryBuilder.Type.PHRASE).tieBreaker((float) 0.5));
    }

    // System.out.println(qb.toString());
    return qb;
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    ReindexCrawlerResult test = new ReindexCrawlerResult();
    test.reIndex();
    try {
      test.es.closeES();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
