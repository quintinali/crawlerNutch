package pd.nutch.textrank;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;

/**
 * TextRank关键词提取
 * 
 * @author hankcs
 */
public class TextRankKeyword {
  public int nKeyword = 10;
  /**
   * 阻尼系数（ＤａｍｐｉｎｇＦａｃｔｏｒ），一般取值为0.85
   */
  static final float d = 0.85f;
  /**
   * 最大迭代次数
   */
  static final int max_iter = 200;
  static final float min_diff = 0.001f;

  public TextRankKeyword() {
    // jdk bug : Exception in thread "main" java.lang.IllegalArgumentException:
    // Comparison method violates its general contract!
    System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
  }

  public String getKeyword(String title, String content) {
    List<Term> termList = HanLP.segment(title + content);
    // System.out.println(termList);
    List<String> wordList = new ArrayList<String>();
    for (Term t : termList) {
      if (shouldInclude(t)) {
        wordList.add(t.word);
      }
    }
    // System.out.println(wordList);
    Map<String, Set<String>> words = new HashMap<String, Set<String>>();
    Queue<String> que = new LinkedList<String>();
    for (String w : wordList) {
      if (!words.containsKey(w)) {
        words.put(w, new HashSet<String>());
      }
      que.offer(w);
      if (que.size() > 5) {
        que.poll();
      }

      for (String w1 : que) {
        for (String w2 : que) {
          if (w1.equals(w2)) {
            continue;
          }

          words.get(w1).add(w2);
          words.get(w2).add(w1);
        }
      }
    }
    // System.out.println(words);
    Map<String, Float> score = new HashMap<String, Float>();
    for (int i = 0; i < max_iter; ++i) {
      Map<String, Float> m = new HashMap<String, Float>();
      float max_diff = 0;
      for (Map.Entry<String, Set<String>> entry : words.entrySet()) {
        String key = entry.getKey();
        Set<String> value = entry.getValue();
        m.put(key, 1 - d);
        for (String other : value) {
          int size = words.get(other).size();
          if (key.equals(other) || size == 0)
            continue;
          m.put(key, m.get(key)
              + d / size * (score.get(other) == null ? 0 : score.get(other)));
        }
        max_diff = Math.max(max_diff, Math
            .abs(m.get(key) - (score.get(key) == null ? 0 : score.get(key))));
      }
      score = m;
      if (max_diff <= min_diff)
        break;
    }
    List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(
        score.entrySet());
    Collections.sort(entryList, new Comparator<Map.Entry<String, Float>>() {
      @Override
      public int compare(Map.Entry<String, Float> o1,
          Map.Entry<String, Float> o2) {
        int ret = 1;
        Float f1 = (Float) o1.getValue();
        Float f2 = (Float) o2.getValue();
        ret = (f1 - f2 >= 0 ? -1 : 1);
        return ret;
      }
    });
    // System.out.println(entryList);
    int listSize = entryList.size();
    int keyNum = nKeyword > listSize ? listSize : nKeyword;
    String result = "";
    for (int i = 0; i < keyNum; ++i) {
      result += entryList.get(i).getKey() + '#';
    }

    return result;
  }

  public static void main(String[] args) {
    // String content = "accrington wikipedia free-encyclopedia-jump navigation
    // search-accrington-accy
    // acce-town-accrington-town-hall-accrington-accrington
    // lancashire-population-49 600-os-grid-reference
    // district-hyndburn-shire-county-lancashire-region-north-west-country-england-sovereign-state-united-kingdom-post-town-accrington-postcode-district
    // code-01254-police-lancashire-fire-lancashire-ambulance-north-west-eu-parliament-north-west-england-uk-parliament-hyndburn-list
    // places england-lancashire-coordinates 53-45-12-n-2-21-50-w
    // 53.75337-n-2.36384-w-53.75337 accrington town hyndburn-borough lancashire
    // england about-4-miles 6-km blackburn 6-miles-10-km-west burnley 13-miles
    // 21-km preston 20-miles 32-km-north manchester-city-centre
    // mostly-culverted-river-hyndburn commonly-abbreviated locals accy town
    // population 45 600 2001-census urban-area population over-80 000
    // accrington former-centre cotton-textile-machinery-industries town
    // hardest-densest-building-bricks world accrington-nori iron which
    // construction empire-state-building foundations blackpool-tower
    // accrington-stanley-f.c haworth-art-gallery which europe
    // s-largest-collection tiffany-glass contents 1-history-1.1-origin
    // name-1.2-early-history-1.3-industrial-revolution-1.4-accrington-pals-2-demography-3-economy-4-regeneration-investment-5-geography-6-transport-7-public-services-8-law-enforcement-9-social-9.1-governance-9.2-health-9.3-media-10-special-events
    // sport-11.1-football-team
    // team-history-11.2-cricket-11.3-golf-11.4-other-sports-12-education-13
    // landmarks-14-notable-residents 15-shops 16-see-also-17-references
    // 18-further 19-external-links edit origin name edit name accrington origin
    // records akarinton 1194 akerunton akerinton-akerynton 1258 acrinton 1292
    // ackryngton 1311 acryngton 1324 2 name acorn
    // anglo-saxon-æcern-meaning-acorn-tun-meaning village 2 southern-part
    // accrington township new-accrington forest blackburnshire presence
    // oak-trees local-place-names broad-oak oak-hill products oak-trees
    // important-food swine farmstead produce 2 anglo-saxon-ᴁcerntun
    // middle-english-akerenton akerinton like 2 also-worth-considering
    // lancashire-dialect-acorn 4 known-old-english-personal-name which
    // first-element frisian-names-akkrum akkeringa-dutch-name-akkerghem
    // personal-name-akker corresponding-old-english-name which accrington
    // 2-early-history edit accrington two-townships which
    // 1507-following-disafforestation those old-accrington new-accrington which
    // 1878 incorporation borough-council 5 settlements medieval-period
    // grange-lane black-abbey-area 5 6 king s-highway which town one-time
    // kings-queens england area hunting forest accrington one four-forests
    // hundred blackburnshire robert-de-lacy manor accrington monks kirkstall
    // 12th-century monks grange inhabitants room locals revenge fire
    // new-building its-contents process three brothers who 5 area town
    // black-abbey possible-reference murders whatever accrington
    // monastic-control long de-lacys monks kirkstall small-chapel tenure
    // convenience those charge tenants records what chapel accrington 1553-5
    // vicar whalley maintenance divine-worship its-own-minister all curate one
    // adjacent-chapels 1717-accrington curate church who month 5-st james
    // s-church 1763 old-chapel parochial-status late-1870
    // 6-industrial-revolution edit section expansion you june-2016
    // around-1830-visitors accrington just-considerable-village 5
    // industrial-revolution large-changes accringtons-location confluence
    // number streams industry number mills town mid-eighteenth-century
    // industrialisation late-eighteenth-century-local-landowners mansions area
    // outskirts settlement mills employees overcrowded-unsanitary-conditions
    // centre 6-industrialisation rapid-population-growth nineteenth-century
    // people north-west-england accrington 7 population 3 266 1811 10 376 1851
    // 43 211 1901-6 its-peak 1911 45 029 8 fast-population-growth slow-response
    // established-church-allowed-non-conformism town mid-nineteenth-century
    // wesleyan primitive-methodist united-free-methodist congregationalist
    // baptist swedenborgian unitarian roman-catholic
    // catholic-apostolic-churches town 5 swedenborgian-church cathedral
    // denomination 8 many-decades textiles-industry
    // engineering-industry-coal-mining central-activities town cotton-mills-dye
    // work inhabitants very-difficult-conditions regular-conflict employers
    // wages conditions 24-april-1826 1 000-men-women many-armed whinney-hill
    // clayton-le-moors speaker sykess-mill higher-grange-lane site
    // modern-police-station magistrats-courts over-60-looms riots accrington
    // oswaldtwistle blackburn darwen rossendale bury chorley end three-days
    // riots 1 139-looms 4-rioters 2-bystanders authorities rossendale
    // 41-rioters death-all whose-sentences 9 1842 riots general-strike town
    // town conditions town population 9 000-people 100 11 15-august-1842
    // situation bands men mills which machinery boiler-plugs water-steam
    // mill-machinery 12-thousands strikers hills one-town another people strike
    // civil-disturbances week 13 strike chartist-movement unsuccessful its-aims
    // 15 early-1860s lancashire-cotton-famine-badly-affected-accrington
    // wider-area its-more-diverse-economy half town s-mill-employees work
    // one-time 17-conditions local-board health 1853 town itself 1878
    // enforcement local-laws town 5 6 accrington-pals-edit further-information
    // recruitment british-army world-war i one-well-known-association town
    // accrington-pals nickname smallest-home-town-battalion volunteers
    // first-world-war pals-battalions peculiarity 1914-18-war lord-kitchener
    // secretary state war recruitment friends-work-mates same-town
    // accrington-pals battalion 11th-east-lancashire-regiment nickname little
    // four-250-strong-companies original-battalion only-one men accrington rest
    // other-east-lancashire-towns burnley blackburn-chorley 18-pals first-day
    // action 1-july-1916 place serre montauban north france 19 part big-push
    // battle somme german-army retreat western-front line late-1914
    // german-defences serre sustained heavy british-shelling preceding-week
    // battalion fierce-resistance 235-men further-350 half battalion half hour
    // similarly-desperate-losses front disastrous-day british-army
    // approximately-19 000-british-soldiers single-day year
    // east-lancashire-regiment new-volunteers all 865-accrington-men
    // world-war-i all names war-memorial imposing-white-stone-cenotaph which
    // oak-hill-park south town cenotaph names 173-local-fatalities world-war-ii
    // trenches which accrington-pals 1-july-1916 john-copse-west village serre
    // memorial accrington-brick war 1986 accrington-corporation-buses
    // regimental-colours red-blue gold-lining mudguards sign mourning citation
    // demography-edit 2001-census population accrington-town 45 600 figure
    // urban-area 78 224 70 442 1991 total accrington church clayton-le-moors
    // great-harwood oswaldtwistle comparison-purposes approximately-same-size
    // aylesbury carlisle guildford scunthorpe-urban-areas borough hyndburn
    // whole population 88 996 accrington-urban-area
    // other-outlying-towns-villages altham baxenden part belthorn huncoat
    // rishton stanhill economy-edit section expansion you june-2016 cotton
    // textile-machinery important-industries town citation noris type
    // iron-hard-engineering-brick huncoat 2013 2015 mills-factories accrington
    // s-past few-factories-garages old-building regeneration-investment-edit
    // council regeneration-plan place which council local-economy plan
    // many-old-shops bus-station memorial accrington-pals town-hall
    // hyndburn-borough-council 10-million town-centre which all-shops
    // town-centre citation new-bus-station plans new-bus-station which
    // george-slynn honour former-hundburn-council-leader january-2013
    // october-2014 21-bus-station 11-july-2016 new-station heavy-criticism
    // traders accrington s-market-town-hall old-station which
    // much-closer-provided-ease access regular-customers 23-24-half
    // blackburn-road more-attractive-shopping-street shops more-trees pavements
    // 25 two-new-phases first-one acorn-park new-houses balconies
    // greener-spaces phoenix-place which new-housing building new overcrowding
    // nearby-mosques citation wood-nook-area highest-amount abandoned-housing
    // hyndburn which sale new-families citation geography-edit section
    // expansion you june-2016-accrington hill-town western-edge pennines bowl
    // hills heights 300-400m hyndburn-accrington-brook centre town
    // hill-settlements-origins economic-foci district spinning-weaving
    // woolen-cloth wool lead-coal other-local-industries geographical
    // 53-46-0-north 2-21 west height sea-level spot-height market-hall which
    // bench-mark side neighbouring-town-hall 441.10-feet highest-height town
    // which baxenden lowest town-hall which most town 200m transport-edit town
    // strong-local-travel-links accrington-railway-station east-lancashire-line
    // trains trains blackpool york recent-changes train-timetables disservice
    // accrington journey-time preston vital-link london scotland up-1.5-hours
    // buses manchester-every-thirty-minutes more-frequent-services other-towns
    // east-lancashire main-road town-centre a680 rochdale whalley town junction
    // seven m65 a680 a56-dual-carriageway which briefly m66-motorway manchester
    // closest-airports manchester-airport 27-miles 43-km blackpool-airport
    // 28-miles 45-km leeds-bradford-airport 30-miles 48-km once-rail-link
    // manchester haslingden-bury 1960s part cuts beeching-report trackbed
    // accrington baxenden linear-treelined-cycleway-footpath train-service
    // manchester todmorden-curve 2015 new-bus-station accrington
    // 26-bus-operators pilkington-bus m-m-coaches accrington holmeswood-coaches
    // rosso transdev-blazefield-subsidiaries-transdev burnley-pendle
    // blackburn-bus-company bus-services town routes places blackburn
    // oswaldtwistle rishton burnley clitheroe 27-however-m-m-coaches business
    // 21st-september-28-public-services edit
    // accrington-library-accrington-library st-james-street carnegie-library
    // 1908 its-stained-glass-window gustav-hiller place inspiration
    // young-jeanette-winterson 29 tesco-supermarket accrington-skate-park which
    // school-holidays broadway accrington-police-station borough hyndburn
    // april-2003 hyndburn-community-fire-station borough hyndburn
    // law-enforcement-edit section expansion you june-2016 town
    // lancashire-constabulary-police-station manchester-road police-station
    // town effort money rising-expenses funding government crime accrington
    // nearby-towns citation social-edit governance edit accrington parliament
    // part constituency hyndburn constituency-boundaries those district
    // same-name accrington first redistribution seats act-1885
    // 1885-general-election accrington-uk-parliament-constituency seat
    // 1983-general-election present-constituency
    // hyndburn-uk-parliament-constituency accrington municipal-borough 1878
    // local-government-act-1972 1974 town part larger-borough hyndburn
    // former-urban-districts oswaldtwistle church clayton-le-moors
    // great-harwood rishton hyndburn 16-wards total 35-councillors
    // its-size-accrington number wards borough hyndburn town milnshaw peel
    // central barnfield-spring-hill-wards some-parts those-wards other-towns
    // borough health-edit accrington-acorn-phcc construction local-hospital
    // accrington-victoria-hospital minor-issues accident emergency
    // royal-blackburn-hospital other-services
    // accrington-pals-primary-health-care-centre
    // accrington-acorn-primary-health-care-centre media-edit chief-publications
    // area weekly-accrington-observer part men-media lancashire-telegraph
    // accrington-observer market-hall special-events edit ron-hill-10k-marathon
    // every-year end march-start april local-olympic-runner britain-ron-hill
    // marathon town local-countrysides council-local-businesses
    // annual-1k-family-run which 2014 2015-more-than-five-hundred-runners race
    // also race local-rotary august-time sport-edit-football-team
    // accrington-stanley-f.c-wall-plaque
    // industry-prudence-conquer-accrington-stanley-f.c 30 football-league 1921
    // formation old";

    String content = "catalog-page pia19439-pia19439 five-new-crater-names mercury-target-name mercury satellite sol our-sun-mission messenger-spacecraft messenger-product-size 2683-x 2175-pixels x-h produced johns-hopkins-university-apl-full-res-tiff pia19439-tif 17.51-mb-full-res-jpeg 599.6-kb-click image download moderately-sized-image jpeg size original original-caption image five-previously-unnamed-craters mercury names messenger s-education-public-outreach-epo-team contest suggestions public competition-website total 3 600-contest-entries semi-final-list 17-names international-astronomical-union-iau consideration iau final-five-crater-names convention mercury s-craters those who significant-contributions humanities winners carolan 83.8 n e turlough-o'carolan irish-musician-composer-1670-1738-enheduanna 48.3 n e author-poet ancient-mesopotamia-karsh-35.6-s 78.9-e yousuf-karsh twentieth-century-armenian-canadian-portrait-photographer-kulthum n 93.5-e umm-kulthum twentieth-century-egyptian-singer songwriter actress-rivera 69.3 n 32.4-e diego-rivera twentieth-century-mexican-painter-muralist animation locations five-newly-named-craters them graphic iau-news-release messenger-mission-news-story additional-details messenger-spacecraft first planet-mercury spacecraft s-seven-scientific-instruments-radio-science-investigation history-evolution solar-system s-innermost-planet mission more-than-four-years orbital-operations messenger 250 000-images extensive-other-data-sets messenger s orbital-mission end spacecraft propellant force solar-gravity surface mercury april-30 2015 information use images messenger-image-use-policy image-credit nasa-johns-hopkins-university-applied-physics-laboratory-carnegie-institution washington-image-addition-date";

    System.out.println(new TextRankKeyword().getKeyword("", content));

  }

  /**
   * 是否应当将这个term纳入计算，词性属于名词、动词、副词、形容词
   * 
   * @param term
   * @return 是否应当
   */
  public boolean shouldInclude(Term term) {
    return CoreStopWordDictionary.shouldInclude(term);
  }
}
