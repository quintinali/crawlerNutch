package pd.nutch.textrank;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.common.Term;

/**
 * TextRank 自动摘要
 * 
 * @author hankcs
 */
public class TextRankSummary {
  /**
   * 阻尼系数（ＤａｍｐｉｎｇＦａｃｔｏｒ），一般取值为0.85
   */
  final double d = 0.85f;
  /**
   * 最大迭代次数
   */
  final int max_iter = 200;
  final double min_diff = 0.001f;
  /**
   * 文档句子的个数
   */
  int D;
  /**
   * 拆分为[句子[单词]]形式的文档
   */
  List<List<String>> docs;
  /**
   * 排序后的最终结果 score <-> index
   */
  TreeMap<Double, Integer> top;

  /**
   * 句子和其他句子的相关程度
   */
  double[][] weight;
  /**
   * 该句子和其他句子相关程度之和
   */
  double[] weight_sum;
  /**
   * 迭代之后收敛的权重
   */
  double[] vertex;

  /**
   * BM25相似度
   */
  BM25 bm25;

  public TextRankSummary(List<List<String>> docs) {
    this.docs = docs;
    bm25 = new BM25(docs);
    D = docs.size();
    weight = new double[D][D];
    weight_sum = new double[D];
    vertex = new double[D];
    top = new TreeMap<Double, Integer>(Collections.reverseOrder());
    solve();
  }

  private void solve() {
    int cnt = 0;
    for (List<String> sentence : docs) {
      double[] scores = bm25.simAll(sentence);
      // System.out.println(Arrays.toString(scores));
      weight[cnt] = scores;
      weight_sum[cnt] = sum(scores) - scores[cnt]; // 减掉自己，自己跟自己肯定最相似
      vertex[cnt] = 1.0;
      ++cnt;
    }
    for (int _ = 0; _ < max_iter; ++_) {
      double[] m = new double[D];
      double max_diff = 0;
      for (int i = 0; i < D; ++i) {
        m[i] = 1 - d;
        for (int j = 0; j < D; ++j) {
          if (j == i || weight_sum[j] == 0)
            continue;
          m[i] += (d * weight[j][i] / weight_sum[j] * vertex[j]);
        }
        double diff = Math.abs(m[i] - vertex[i]);
        if (diff > max_diff) {
          max_diff = diff;
        }
      }
      vertex = m;
      if (max_diff <= min_diff)
        break;
    }
    // 我们来排个序吧
    for (int i = 0; i < D; ++i) {
      top.put(vertex[i], i);
    }
  }

  /**
   * 获取前几个关键句子
   * 
   * @param size
   *          要几个
   * @return 关键句子的下标
   */
  public int[] getTopSentence(int size) {
    Collection<Integer> values = top.values();
    size = Math.min(size, values.size());
    int[] indexArray = new int[size];
    Iterator<Integer> it = values.iterator();
    for (int i = 0; i < size; ++i) {
      indexArray[i] = it.next();
    }
    return indexArray;
  }

  /**
   * 简单的求和
   * 
   * @param array
   * @return
   */
  private static double sum(double[] array) {
    double total = 0;
    for (double v : array) {
      total += v;
    }
    return total;
  }

  public static void main(String[] args) {
    String document = "(322756) 2001 CK32 - Wikipedia (322756) 2001 CK 32 From Wikipedia, the free encyclopedia Jump to: navigation , search 2001 CK 32 Discovery Discovered by LINEAR Discovery date 13 February 2001 Designations MPC designation 2001 CK 32 Minor planet category Aten asteroid , [1] [2] Mercury grazer , Venus crosser , Earth crosser Orbital characteristics [2] [3] [4] Epoch 13 January 2016 ( JD 2457400.5) Uncertainty parameter 0 Observation arc 4777 days (13.08 yr) Aphelion 1.002762662  AU (150.0111591  Gm ) Perihelion 0.44776848 AU (66.985211 Gm) Semi-major axis 0.725265571 AU (108.4981851 Gm) Eccentricity 0.3826145 Orbital period 0.62 yr (225.6 d ) Mean anomaly 197.81721 ° Mean motion 1.5957266°/day Inclination 8.1302858° Longitude of ascending node 109.44400° Argument of perihelion 234.11841° Earth  MOID 0.0769248 AU (11.50779 Gm) Jupiter  MOID 4.36644 AU (653.210 Gm) Jupiter Tisserand parameter 7.857 Physical characteristics Dimensions 800 m [a] [5] Absolute magnitude  (H) 19.0 [2] (322756) 2001 CK 32 , also written 2001 CK32 , is a transient Venus co-orbital , [6] [7] but also a Mercury grazer and an Earth crosser . It was once designated as a potentially hazardous asteroid . [ citation needed ] Contents 1 See also 2 Notes 3 References 4 External links See also [ edit ] 2002 VE 68 2012 XE 133 2013 ND 15 Notes [ edit ] ^ This is assuming an albedo of 0.25–0.05. References [ edit ] ^ List Of Aten Minor Planets ^ a b c \"(322756) 2001 CK32\" . JPL Small-Body Database . Jet Propulsion Laboratory . SPK-ID:  2322756 . Retrieved 8 April 2016 .   ^ AstDys-2 on 2001 CK32 Retrieved 2012-08-25 ^ NEODyS-2 on 2001 CK32 Retrieved 2012-08-25 ^ Absolute-magnitude conversion table (H) ^ Transient co-orbital asteroids ^ de la Fuente Marcos, C.; de la Fuente Marcos, R. \"Asteroid 2012 XE133, a transient companion to Venus\" . Monthly Notices of the Royal Astronomical Society . 432 (2): 886–893. arXiv : 1303.3705 . Bibcode : 2013MNRAS.432..886D . doi : 10.1093/mnras/stt454 .   Further reading Understanding the Distribution of Near-Earth Asteroids Bottke, W. F., Jedicke, R., Morbidelli, A., Petit, J.-M., Gladman, B. 2000, Science , Vol. 288, Issue 5474, pp. 2190–2194. A Numerical Survey of Transient Co-orbitals of the Terrestrial Planets Christou, A. A. 2000, Icarus , Vol. 144, Issue 1, pp. 1–20. Debiased Orbital and Absolute Magnitude Distribution of the Near-Earth Objects Bottke, W. F., Morbidelli, A., Jedicke, R., Petit, J.-M., Levison, H. F., Michel, P., Metcalfe, T. S. 2002, Icarus , Vol. 156, Issue 2, pp. 399–433. Transient co-orbital asteroids Brasser, R., Innanen, K. A., Connors, M., Veillet, C., Wiegert, P., Mikkola, S., Chodas, P. W. 2004, Icarus , Vol. 171, Issue 1, pp. 102–109. The population of Near Earth Asteroids in coorbital motion with Venus Morais, M. H. M., Morbidelli, A. 2006, Icarus , Vol. 185, Issue 1, pp. 29–38. Asteroid 2012 XE133: a transient companion to Venus de la Fuente Marcos, C., de la Fuente Marcos, R. 2013, Monthly Notices of the Royal Astronomical Society , Vol. 432, Issue 2, pp. 886–893. External links [ edit ] 2001 CK 32 data at MPC List of Potentially Hazardous Asteroids (PHAs) (322756) 2001 CK32 at the JPL Small-Body Database Discovery   · Orbit diagram   · Orbital elements   · Physical parameters v t e Small Solar System bodies Minor planets Designation Groups Moons Meanings of names Pronunciation of names Asteroids Aten asteroid Asteroid belt Families Jupiter Trojans Near-Earth Spectral types Distant minor planets Centaurs Damocloids Neptune trojans Trans-Neptunian objects Detached Kuiper belt Oort cloud Scattered disc Comets Extinct Great Lost Main-belt Non-periodic Periodic Sungrazing Meteoroids Bolide Dust Fireball Meteor Meteorite Tektite Lists / categories Asteroid groups and families Asteroid moons Binary asteroids Minor planets v t e Venus Geography General Arachnoid Atmosphere Dune fields Features Regions Regio Alpha Regio Asteria Regio Beta Regio Ovda Regio Terrae Aphrodite Terra Ishtar Terra Lada Terra Mountains and volcanoes Abeona Mons Akna Montes Anala Mons Artemis Corona Ba'het Corona Boala Corona Ciuacoatl Mons Fand Mons Fotla Corona Gula Mons Heng-O Corona Iaso Tholus Irnini Mons Jaszai Patera Maat Mons Maxwell Montes Nightingale Corona Onatah Corona Quetzalpetlatl Corona Pancake dome Renpet Mons Sacajawea Patera Sachs Patera Sapas Mons Santa Corona Scalloped margin dome Siddons Patera Sif Mons Skadi Mons Theia Mons Ushas Mons Zisa Corona List of coronae on Venus Plains and plateaus Guinevere Planitia Lakshmi Planum Sedna Planitia Canyons and valleys Aikhulu Chasma Artemis Chasma Baltis Vallis Dali Chasma Devana Chasma Diana Chasma Ganiki Chasma Craters Addams Adivar Agnesi Alcott Ariadne Aurelia Balch Barton Buck Cleopatra Cunitz Danilova De Lalande Dickinson Goeppert-Mayer Golubkina Grimke Gregory Guilbert Isabella Jeanne Maria Celeste Mariko Mead Meitner Merit Ptah Mona Lisa Nanichi Riley Ruth Stefania Wanda Wheatley Yablochkina Other Geodynamics Geology Surface features Venusquake Moons Neith (hypothetical moon) Astronomy General Aspects Orbit Phases Transits 1639 1769 1874 1882 2004 2012 Asteroids Venus-crosser asteroid 2002 VE 68 Exploration Past Sputnik programme Sputnik 7 Venera program Venera 1 Sputnik 19 Sputnik 20 Sputnik 21 Kosmos 21 Venera 1964A Kosmos 27 Venera 2 Venera 3 Kosmos 96 Venera 4 Kosmos 167 Venera 5 Venera 6 Venera 7 Venera 8 Kosmos 482 Venera 9 Venera 10 Venera 11 Venera 12 Venera 13 Venera 14 Venera 15 Venera 16 Mariner program Mariner 1 Mariner 2 Mariner 5 Mariner 10 Zond program Zond 1 Pioneer Venus Pioneer Venus Orbiter /Pioneer 12 Pioneer Venus Multiprobe /Pioneer 13 Vega program Vega 1 Vega 2 Magellan Galileo Cassini–Huygens IKAROS MESSENGER Shin'en Venus Express Current Akatsuki Planned BepiColombo Venera-D Proposed European Venus Explorer Mercury-P Venus In Situ Explorer Indian Venusian orbiter mission Proposed manned Colonization Inspiration Mars (flyby) Terraforming Other Geological mapping of Venus Artificial objects on Venus Related Cytherean Fiction Hesperus Life Mythology Phosphorus Venusians Book Category Commons Portal Retrieved from \" https://en.wikipedia.org/w/index.php?title=(322756)_2001_CK32&oldid=714230020 \" Categories : Co-orbital minor planets Aten asteroids Venus-crosser asteroids Earth-crosser asteroids Astronomical objects discovered in 2001 Numbered minor planets Discoveries by LINEAR Hidden categories: All articles with unsourced statements Articles with unsourced statements from January 2016 JPL Small-Body Database ID different from Wikidata Navigation menu Personal tools Not logged in Talk Contributions Create account Log in Namespaces Article Talk Variants";
    System.out.println(TextRankSummary.getTopSentenceList(document, 3));
  }

  /**
   * 将文章分割为句子
   * 
   * @param document
   * @return
   */
  static List<String> spiltSentence(String document) {
    List<String> sentences = new ArrayList<String>();
    if (document == null)
      return sentences;
    for (String line : document.split("[\r\n]")) {
      line = line.trim();
      if (line.length() == 0)
        continue;
      for (String sent : line.split("[，,。:：“”？?！!；;]")) {
        sent = sent.trim();
        if (sent.length() == 0)
          continue;
        sentences.add(sent);
      }
    }

    return sentences;
  }

  /**
   * 是否应当将这个term纳入计算，词性属于名词、动词、副词、形容词
   * 
   * @param term
   * @return 是否应当
   */
  public static boolean shouldInclude(Term term) {
    return CoreStopWordDictionary.shouldInclude(term);
  }

  /**
   * 一句话调用接口
   * 
   * @param document
   *          目标文档
   * @param size
   *          需要的关键句的个数
   * @return 关键句列表
   */
  public static List<String> getTopSentenceList(String document, int size) {
    List<String> sentenceList = spiltSentence(document);
    List<List<String>> docs = new ArrayList<List<String>>();
    for (String sentence : sentenceList) {
      List<Term> termList = HanLP.segment(sentence);
      List<String> wordList = new LinkedList<String>();
      for (Term term : termList) {
        if (shouldInclude(term)) {
          wordList.add(term.word);
        }
      }
      docs.add(wordList);
    }
    TextRankSummary textRankSummary = new TextRankSummary(docs);
    int[] topSentence = textRankSummary.getTopSentence(size);
    List<String> resultList = new LinkedList<String>();
    for (int i : topSentence) {
      resultList.add(sentenceList.get(i));
    }
    return resultList;
  }
}
