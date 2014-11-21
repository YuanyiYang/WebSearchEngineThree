package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import org.jsoup.Jsoup;

/**
 * Provide query expansion results of the given query
 */

public class QueryRepresentation {

  // private static final String queryFile = "/queries.tsv/"; //query file
  // location
  private static final String WORKINGDIR = System.getProperty("user.dir");

  private LinkedHashMap<String, Float> terms = new LinkedHashMap<String, Float>();
  private Vector<ScoredDocument> results = null; // the ranked document
  private int termNum; // restricted number of terms
  private float total_tf = 0.0f; // total term frequence in the top k results

  // Utilized for sorting
  class kvpair implements Comparable<kvpair> {
    public String key;
    public float value;

    public kvpair(String key, Float value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(kvpair kvp) {
      if (this.value < kvp.value) {
        return 1;
      } else if (this.value == kvp.value) {
        return 0;
      } else {
        return -1;
      }
    }
  }

  // Get the top M term which has the largest term probability
  public String QueryExpansion() {
    // build the term freq map for the scored documents set
    System.out.println(1);
    for (ScoredDocument sd : results) {
      Document d = sd.getDocument();
      Map<String, Float> topterms = d.getTopTerms();

      for (String term : topterms.keySet()) {
        if (terms.containsKey(term)) {
          terms.put(term, terms.get(term) + 1.0f);
        } else {
          terms.put(term, 1.0f);
        }
      }

      total_tf += d.getTermFrequency();
    }
    System.out.println(2);
    // calculate the probability
    for (String term : terms.keySet()) {
      terms.put(term, (terms.get(term) / total_tf));
    }
    System.out.println(3);
    // sort the result and truncate the result based on ranking and parameter
    if (terms.keySet().size() > termNum) {
      List<kvpair> temp = new ArrayList<kvpair>();
      for (String key : terms.keySet()) {
        temp.add(new kvpair(key, terms.get(key)));
      }

      Collections.sort(temp);
      terms.clear();

      // put the top m terms into the terms map
      for (int i = 0; i < termNum; i++) {
        terms.put(temp.get(i).key, temp.get(i).value);
      }
      temp = null;
    }

    normalize(terms);

    StringBuilder result = new StringBuilder();

    for (String term: terms.keySet()) {
      result.append('<' + term + "><" + terms.get(term).toString() + ">\n");
    }
    return new String(result);
  }

  // Normalize the results
  private void normalize(LinkedHashMap<String, Float> terms) {
    float sum = 0.0f;
    for (String key : terms.keySet()) {
      sum += terms.get(key);
    }
    for (String key : terms.keySet()) {
      float temp = terms.get(key) / sum;
      terms.put(key, temp);
    }
  }

  private boolean isCharacterNumber(char c) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
      return true;
    } else {
      return false;
    }
  }

  // tokenizer for the input string
  private Vector<String> Tokenization(String s) {
    Vector<String> r = new Vector<String>();
    if (s.length() == 0) {
      return r;
    }
    int decide = 0;
    StringBuilder sb = new StringBuilder();

    int id = 0;
    while (id < s.length() && s.charAt(id) < 128
        && !isCharacterNumber(s.charAt(id))) {
      id++;
    }

    if (id != s.length()) {
      if (s.charAt(id) < 128) { // ASC code
        decide = 0;
        sb.append(s.charAt(id));
      } else { // n-ASC code
        r.add("" + s.charAt(id));
        decide = 1;
      }
    } else {
      return r;
    }

    for (int i = id + 1; i < s.length(); i++) {
      char c = s.charAt(i);
      if (!isCharacterNumber(c) && c < 128) { // if there is a punctuation here
        String s_temp = new String(sb);
        if (s_temp.length() != 0 && !StopWordsList.isStopWord(s_temp)) {
          r.add(s_temp);
        }
        sb.delete(0, sb.capacity());
        decide = 0;
        continue;
      }
      if (c < 128 && isCharacterNumber(c)) { // ASCII code
        sb.append(c);
        decide = 0;
      } else { // n-ASCII code
        if (decide == 0) {
          r.add(new String(sb));
          r.add("" + c);
          sb.delete(0, sb.capacity());
        } else {
          r.add("" + c);
        }
        decide = 1;
      }
    }

    String s_temp = new String(sb);
    if (s_temp.length() != 0 && !StopWordsList.isStopWord(s_temp)) {
      r.add(s_temp);
    }

    return r;
  }

  public QueryRepresentation(Vector<ScoredDocument> results, int termNum) {
    this.results = results;
    this.termNum = termNum;
    System.out.println("Rebuilding the query");
  }
}
