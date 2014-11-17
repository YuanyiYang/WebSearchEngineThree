package edu.nyu.cs.cs2580;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Vector;

/*****
 * 
 * Provide query expansion results of the given query
 * 
 * @author xzysolitaire
 *
 */

public class QueryRepresentation {
  
  //private static final String queryFile = "/queries.tsv/"; //query file location
  private static final String outputDir = "";    //the outputFile directory
  
  private String query;
  private LinkedHashMap<String, Float> terms 
            = new LinkedHashMap<String, Float>();
  private IndexerInvertedOccurrence indexer;
  private Vector<ScoredDocument> results = null;  //the ranked document
  private int termNum;   //restricted number of terms
  private double total_tf = 0.0; //total term frequence in the top k results
  
  //Utilized for sorting
  class kvpair implements Comparable<kvpair> {
    public String key;
    public float value;
    
    public kvpair(String key, Float value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(kvpair kvp) {
      if (this.value <= kvp.value) {
        return 1;
      } else {
        return -1;
      }
    }
  }
  
  //Get the top M term which has the largest term probability
  public void QueryExpansion() {
    for (ScoredDocument sd: results) {
      Document d = sd.getDocument();
      /*************
       * TODO: How to retrieve the term feature in this question?
       *       get the term feature of the given document
       */
    }
    
    if (terms.keySet().size() > termNum) {
      List<kvpair> temp = new ArrayList<kvpair>();
      for (String key: terms.keySet()) {
        temp.add(new kvpair(key, terms.get(key)));
      }
      
      Collections.sort(temp);
      terms.clear();
      
      //put the top m terms into the terms map
      for (int i = 0; i < termNum; i++) {
        terms.put(temp.get(i).key, temp.get(i).value);
      }
      temp = null;
    }
    
    normalize(terms);
  }
  
  //Normalize the results 
  private void normalize(LinkedHashMap<String, Float> terms) {
    float sum = 0.0f;
    for (String key: terms.keySet()) {
      sum += terms.get(key);
    }
    for (String key: terms.keySet()) {
      float temp = terms.get(key) / sum;
      terms.put(key, temp);
    }
  }
  
  //Write the query expansion results to the output file
  public void writeToFile() throws IOException {
    String outputFile = outputDir + query;
    OutputStreamWriter writer = 
        new OutputStreamWriter(new FileOutputStream(outputFile, false));
    
    for (String term: terms.keySet()) {
      writer.write(term + ' ' + terms.get(term).toString() + '\n');
    }
    writer.close();
  }
  
  public QueryRepresentation(String query,
                             Vector<ScoredDocument> results,
                             IndexerInvertedOccurrence indexer,
                             int termNum) {
    this.query = query;
    this.results = results;
    this.indexer = indexer;
    this.termNum = termNum;
  }
}
