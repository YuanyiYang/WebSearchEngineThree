package edu.nyu.cs.cs2580;

import java.util.Scanner;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 * ["new york city"], the presence of the phrase "new york city" must be
 * recorded here and be used in indexing and ranking.
 */
public class QueryPhrase extends Query {

  public Vector<Vector<String>> _phrases = new Vector<Vector<String>>();
  public Vector<String> phrase_v = new Vector<String>(); // phrase string vector
  public Vector<String> phrase_word_v = new Vector<String>(); // all the phrases
                                                              // and terms

  public QueryPhrase(String query) {
    super(query);
    processQuery();
  }

  public Vector<Vector<String>> getPhrases() {
    return _phrases;
  }

  // put all phrases and tokens as separated terms (one phrase as a term)
  public Vector<String> getPhraseTerm() {
    return phrase_word_v;
  }

  // get the number of words in the query (used for QL_Ranker)
  public int getQuerySize() {
    int size = _tokens.size();
    for (Vector<String> v_s : _phrases) {
      size += v_s.size();
    }
    return size;
  }

  // concatenate all the terms in a phrase as a string
  private String getPhraseString(Vector<String> v) {
    if (v.size() == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    sb.append(v.get(0));
    for (int i = 1; i < v.size(); i++) {
      sb.append(' ' + v.get(i));
    }

    return new String(sb);
  }

  public Vector<String> parseQueryString(String s) {
    Stemmer stemmer = new Stemmer();
    Vector<String> r = new Vector<String>();
    if (s.length() == 0) {
      return r;
    }
    int decide;
    StringBuilder sb = new StringBuilder();

    sb.append(s.charAt(0));
    if (s.charAt(0) < 128) { // ASC code
      decide = 0;
    } else { // non-ASC code
      decide = 1;
    }

    for (int i = 1; i < s.length(); i++) {
      char c = s.charAt(i);
      if ((c < 128 && decide == 0) || (c >= 128 && decide == 1)) {
        sb.append(c);
      } else {
        String temp = new String(sb);
        if (decide == 0) {
          if (!StopWordsList.isStopWord(temp)) {
            r.add(stemmer.stemStringInstance(temp));
          }
        } else {
          r.add(temp);
        }
        sb.delete(0, sb.capacity());
        decide = 0;
        sb.append(c);
        if (c < 128) {
          decide = 0;
        } else {
          decide = 1;
        }
      }
    }

    if (new String(sb).length() != 0) {
      String temp = new String(sb);
      if (decide == 0 && !StopWordsList.isStopWord(temp)) {
        r.add(stemmer.stemStringInstance(temp));
      } else if (decide == 1) {
        r.add(temp);
      }
    }

    return r;
  }

  public static Vector<String> parseString(String s) {
    Vector<String> r = new Vector<String>();
    Stemmer stemmer = new Stemmer();
    if (s.length() == 0) {
      return r;
    }
    int decide;
    StringBuilder sb = new StringBuilder();

    if (s.charAt(0) < 128) { // ASC code
      decide = 0;
      sb.append(s.charAt(0));
    } else {// non-ASC code
      r.add("" + s.charAt(0));
      decide = 1;
    }

    for (int i = 1; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c < 128) { // ASCII code
        sb.append(c);
        decide = 0;
      } else { // non-ASCII code
        if (decide == 0) {
          String temp = new String(sb);
          if (!StopWordsList.isStopWord(temp)) {
            r.add(stemmer.stemStringInstance(temp));
          }
          r.add("" + c);
          sb.delete(0, sb.capacity());
        } else {
          r.add("" + c);
        }
        decide = 1;
      }
    }

    if (new String(sb).length() != 0) {
      String temp = new String(sb);
      if (!StopWordsList.isStopWord(temp)) {
        r.add(stemmer.stemStringInstance(temp));
      }
    }

    return r;
  }

  @Override
  public void processQuery() {
    if (_query == null) {
      return;
    }

    String query = new String(_query);
    Pattern p = Pattern.compile("\".+?\""); // match the phrase
    Matcher m = p.matcher(query);

    // get the phrases in the query
    while (m.find()) {
      String phrase = m.group();
      phrase = phrase.substring(1, phrase.length() - 1);
      Vector<String> term = new Vector<String>();
      Scanner s = new Scanner(phrase);
      while (s.hasNext()) {
        String str = s.next();
        term.addAll(parseQueryString(str));
      }

      if (term.size() == 1) {
        _tokens.add(term.get(0));
      } else if (term.size() > 1) {
        _phrases.add(term);
      }
      s.close();
    }

    // replace the phrases
    p = Pattern.compile("\".+?\"");
    m = p.matcher(query);
    String str = m.replaceAll("");
    Scanner s = new Scanner(str);
    while (s.hasNext()) {
      String st = s.next();
      Vector<String> term = parseString(st);
      _tokens.addAll(term);
    }
    s.close();

    // put all the phrases and tokens into the phrase_token_v
    for (Vector<String> v : _phrases) { // phrases
      String temp = getPhraseString(v);
      if (temp.length() != 0) {
        phrase_word_v.add(temp);
      }
    }
    for (String s_temp : _tokens) { // tokens
      phrase_word_v.add(s_temp);
    }

    // ***Dynamically decide whether to remove stopword***
    // if the query contains only the stopword, then take the original query
    if (phrase_word_v.size() == 0) {
      p = Pattern.compile("\".+?\"");
      m = p.matcher(query);
      while (m.find()) {
        String phrase = m.group();
        phrase = phrase.substring(1, phrase.length() - 1);
        phrase_word_v.add(phrase);
      }
      p = Pattern.compile("\".+?\"");
      m = p.matcher(query);
      String temp = m.replaceAll("");
      Scanner scanner = new Scanner(temp);
      while (scanner.hasNext()) {
        phrase_word_v.add(scanner.next());
      }
      _tokens.clear();
      _phrases.clear();
      scanner.close();
    }
  }
}
