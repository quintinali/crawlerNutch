package pd.nutch.crawler;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import pd.nutch.driver.ESdriver;

public class GoldKeywordExtractor {

  String[] vocabularies1 = new String[] { "Asteroid class",
      "Atmospheric breakup", "Asteroid" };

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

  public void extractKeyWordFromGoldenText(ESdriver es, String index,
      String type) {

    es.createBulkProcesser();

    BoolQueryBuilder bq = this.createVocabularyQuery();
    SearchRequestBuilder scrollBuilder = es.client.prepareSearch(index)
        .setTypes(type).setScroll(new TimeValue(60000)).setQuery(bq)
        .setSize(100).setExplain(true);

    SearchResponse scrollResp = scrollBuilder.execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();

        /*if (!hit.getId()
            .equals("https://en.wikipedia.org/wiki/Atmospheric_entry")) {
          continue;
        }*/

        List<String> goldenKeywordList = new ArrayList<String>();
        Explanation totalexplain = hit.getExplanation();
        Explanation[] all_explains = totalexplain.getDetails();
        for (Explanation explain : all_explains) {
          Explanation[] nested_level1_explains = explain.getDetails();

          if (nested_level1_explains == null) {
            continue;
          }

          for (Explanation nested_level1_explain : nested_level1_explains) {
            Explanation[] subexplains = nested_level1_explain.getDetails();
            if (subexplains != null && subexplains.length > 0) {
              for (Explanation nestedexplain : subexplains) {
                String description = nestedexplain.getDescription();
                description = description.replaceAll("weight\\(content\\:", "");
                int pos = description.indexOf(" in ");
                String keyword = description.substring(0, pos);
                if (keyword.startsWith("\"")) {
                  keyword = keyword.substring(1, keyword.length() - 1);
                }
                goldenKeywordList.add(keyword);
              }
            }
          }
        }

        List<String> deduped = goldenKeywordList.stream().distinct()
            .collect(Collectors.toList());

        String goldKeywords = String.join(",", deduped);
        // System.out.println(goldKeywords);

        UpdateRequest ur = null;
        try {
          ur = new UpdateRequest(index, type, hit.getId()).doc(jsonBuilder()
              .startObject().field("gold_keywords", goldKeywords).endObject());
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        es.bulkProcessor.add(ur);
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();

      if (scrollResp.getHits().getHits().length == 0)

      {
        break;
      }
    }

    es.destroyBulkProcessor();

  }

  private BoolQueryBuilder createVocabularyQuery() {

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
}
