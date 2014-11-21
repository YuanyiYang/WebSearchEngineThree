package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3 based on your {@code RankerFavorite}
 * from HW2. The new Ranker should now combine both term features and the
 * document-level features including the PageRank and the NumViews. 
 */
public class RankerComprehensive extends Ranker {
  private RankerFavorite rf; 
  
  public RankerComprehensive(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    rf = new RankerFavorite(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) {
	  _indexer.buildInvertMap(query);
    Vector<ScoredDocument> r = rf.runQuery(query, numResults * 3);
    Vector<ScoredDocument> result = new Vector<ScoredDocument>();
    Map<String, Float> pr = new HashMap<String, Float>();
    Map<String, Float> nv = new HashMap<String, Float>();
    
    for (ScoredDocument sd: r) {
      Document d = sd.getDocument();
      pr.put(d.getUrl(), sd.getPageRank());
      nv.put(d.getUrl(), (float)sd.getNumview());
    }
    
    pr = normalizeByRank(MapUtil.sortedByValue(pr));
    nv = normalizeByRank(MapUtil.sortedByValue(nv));
    Map<String, Float> merge = new HashMap<String, Float>();
    
    for (ScoredDocument sd: r) {
      String docurl = sd.getDocument().getUrl();
      merge.put(docurl, pr.get(docurl) + nv.get(docurl));
    }
    
    merge = MapUtil.sortedByValue(pr);
    int i = 0;
    for (String docurl: merge.keySet()) {
      if (i < numResults) {
        for (ScoredDocument sd: r) {
          if (sd.getDocument().getUrl().equals(docurl)) {
            result.add(sd);
            break;
          }
        }        
        i ++;
      } else {
        break;
      }
    }
    
    return result;
  }
  
  //normalize the LinkedHashMap by its rank using 1 / log(rank)
  private LinkedHashMap<String, Float> normalizeByRank(Map<String, Float> map) {
    LinkedHashMap<String, Float> r = new LinkedHashMap<String, Float>();
    int i = 1;
    for (String term: map.keySet()) {
      r.put(term, (float) (1.0f / Math.log(i + 1)));
      i ++;
    }
    
    return r;
  }
}
