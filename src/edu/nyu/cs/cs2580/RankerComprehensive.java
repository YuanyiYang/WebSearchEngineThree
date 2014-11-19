package edu.nyu.cs.cs2580;

import java.util.HashMap;
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
    Vector<ScoredDocument> r = rf.runQuery(query, numResults * 3);
    Map<String, Double> pr = new HashMap<String, Double>();
    Map<String, Double> nv = new HashMap<String, Double>();
    
    for (ScoredDocument sd: r) {
      Document d = sd.getDocument();
      pr.put(d.getUrl(), d.getPageRank());
      
    }
    
    return null;
  }
}
