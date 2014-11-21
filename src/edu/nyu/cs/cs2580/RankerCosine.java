package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

public class RankerCosine extends Ranker {
  
  static private final String docTermFreqFile = "";
  private static int numDocs;

  public RankerCosine(Options options, CgiArguments arguments,
      Indexer indexer) {
    super(options, arguments, indexer);
    numDocs = _indexer.numDocs();
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  @Override
  public Vector<ScoredDocument> runQuery(Query query, int numResults) throws IOException {
    _indexer.buildInvertMap(query);
    Map<String, Double> queryTerm = new HashMap<String, Double>();  //query term-->freq map
    
    Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
    Vector<ScoredDocument> results = new Vector<ScoredDocument>();
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
    
    //read the doc_term_freq file
    BufferedReader br;
    br = new BufferedReader(new FileReader(docTermFreqFile));

    String line;
    while ((line = br.readLine()) != null) {
      Scanner s = new Scanner(line);
      int docid = Integer.parseInt(s.next());
      Map<String, Double> docTerm = new HashMap<String, Double>();
      
      while (s.hasNext()) {
        String term = s.next();
        double freq = (double)Integer.parseInt(s.next());
        docTerm.put(term, freq);
      }
      
      //if the document contains the term in the query 
      if (checkIntersection(queryTerm.keySet(), docTerm.keySet())) {
        double score = computeCosineSimilarity(queryTerm, docTerm);
        ScoredDocument s_d = new ScoredDocument(_indexer.getDoc(docid), score); 
        s_d.setPageRank(_indexer.pageRankValueForDocID(docid));
        s_d.setNumview(_indexer.numviewForDocID(docid));
        rankQueue.add(s_d);
        if (rankQueue.size() > numResults) {
          rankQueue.poll();
        }
      }
      s.close();
    }
    
    ScoredDocument scoredDoc = null;
    while ((scoredDoc = rankQueue.poll()) != null) {
      results.add(scoredDoc);
    }
    Collections.sort(results, Collections.reverseOrder());
    
    br.close();
    return results;
  }
  
  //check if the document contains terms in the query
  private boolean checkIntersection(Set<String> queryTerm, Set<String> doc_term) {
    for (String qt: queryTerm) {
      if (doc_term.contains(qt)) {
        return true;
      }
    }
    return false;
  }
  
  //compute the consine similarity between two documents
  public double computeCosineSimilarity(Map<String, Double> queryTerm, Map<String, Double> docTerm) {
    double up = 0.0, down_q = 0.0, down_d = 0.0;
    
    for (String term: queryTerm.keySet()) {
      if (docTerm.keySet().contains(term)) {
        double idf = getIdf(term);
        up += idf * idf 
              * queryTerm.get(term) 
              * docTerm.get(term);
      }
    }
    down_q = getSqrtSum(queryTerm);
    down_d = getSqrtSum(docTerm);
    
    return up / (down_q * down_d);
  }
  
  private double getSqrtSum(Map<String, Double> termFreq) {
    double r = 0.0;
    for (String term: termFreq.keySet()) {
      double idf = getIdf(term);
      r += idf * idf * termFreq.get(term) * termFreq.get(term);
    }
    return Math.sqrt(r);
  }
  
  private double getIdf(String term) {
    double df = (double)_indexer.corpusDocFrequencyByTerm(term);
    
    if (df == 0.0) {
      return 0.0;
    } else {
      return Math.log((double)numDocs / df) / Math.log(2.0);      
    }
  }
}
