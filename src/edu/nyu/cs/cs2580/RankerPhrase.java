package edu.nyu.cs.cs2580;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

public class RankerPhrase extends Ranker {

  protected RankerPhrase(Options options, CgiArguments arguments,
      Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults)
      throws FileNotFoundException, NumberFormatException, IOException {
    _indexer.buildInvertMap(query);
    Map<String, Double> queryTerm = new HashMap<String, Double>();  //query term-->freq map
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
    int numDocs = _indexer.numDocs();
    
    QueryPhrase qp = new QueryPhrase(query._query);
    
    for (String term: qp.getTokens()) {
      if (queryTerm.containsKey(term)) {
        queryTerm.put(term, queryTerm.get(term) + 1.0);
      } else {
        queryTerm.put(term, 1.0);
      }
    }
    for (Vector<String> v: qp._phrases) {
      for (String term: v) {
        if (queryTerm.containsKey(term)) {
          queryTerm.put(term, queryTerm.get(term) + 1.0);
        } else {
          queryTerm.put(term, 1.0);
        }
      }
    }
    
    //the query contains only one word, use unigram
    if (queryTerm.keySet().size() == 1) {
      String term = "";
      for (String s: queryTerm.keySet()) {
        term = s;
      }
      for (int i = 1; i <= numDocs; i++) {
        double score = _indexer.documentTermFrequency(term, i);
      }
    } else { //use nextDoc to get the number of bigrams
      
    }
      
    return null;
  }

}
