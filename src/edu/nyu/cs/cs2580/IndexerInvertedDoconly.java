package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;

import com.google.common.base.CharMatcher;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {

  private static final long serialVersionUID = -8386259220360593185L;
  private final String PARTS = "/parts/";
  private final String WORKINGDIR = System.getProperty("user.dir");

  // Maps each term to their integer representation
  Map<String, Integer> _dictionary = new HashMap<String, Integer>();
  // All unique terms appeared in corpus. Offsets are integer representations.
  // List<String> _terms = new ArrayList<String>();

  // Term document frequency, key is the integer representation of the term and
  // value is the number of documents the term appears in.
  private Map<Integer, Integer> _termDocFrequency = new HashMap<Integer, Integer>();
  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  private Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  // Stores all Document in memory.(all instances are documentIndexed)
  List<Document> _documents = new ArrayList<Document>();

  // In-Memory Indexer: Key is the integer representation of the term and value
  // is the
  // a List(detail implementation is ArrayList) contains all the document Id in
  // which
  // the term appears change the access type from private to default for test
  // purpose
  transient Map<Integer, List<Integer>> invertedMap = new HashMap<Integer, List<Integer>>();

  Map<String, Integer> urlToDoc = new HashMap<String, Integer>();

  // For compress purpose. Only used to test in this indexer
  // byte[] invertedByte = null;

  // key is integer representation of the term
  // value is a map with which the key is integer representation of the
  // documents and value is occurences of the term in the document
  private Map<Integer, Map<Integer, Integer>> documentTermOccurencesMap = new HashMap<Integer, Map<Integer, Integer>>();

  // cache the index offset of the inverted list
  Map<Integer, Integer> invertedListIndex = new HashMap<Integer, Integer>();

  public IndexerInvertedDoconly() {
  }

  public IndexerInvertedDoconly(Options options) {
    super(options);
    System.out.println("Using Indexer: " + this.getClass().getSimpleName());
  }

  /**
   * Constructs the index from the corpus file.
   * 
   * @throws IOException
   */
  @Override
  public void constructIndex() throws IOException {
    String corpusFile = _options._corpusPrefix;
    corpusFile = WORKINGDIR + "/" + corpusFile;
    System.out.println("Construct index from: " + corpusFile);
    long start = System.currentTimeMillis();
    buildIndex(corpusFile);
    long end = System.currentTimeMillis();
    System.out.println("Constructing Time: " + (end - start) / 1000
        + " seconds");
    System.out.println("Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
    String indexFile = WORKINGDIR + "/" + _options._indexPrefix
        + "/indexerInvertedDoconly.idx";
    // System.out.println("Term size " + _terms.size());
    try {
      // using the Runtime exec method:
      StringBuilder sb = new StringBuilder();
      sb.append("cat ");
      sb.append(WORKINGDIR);
      sb.append(PARTS);
      sb.append("*");
      sb.append(" | sort -k1,1 -n > ");
      sb.append(WORKINGDIR);
      sb.append("/data/catTemp;");
      sb.append(" rm ");
      sb.append(WORKINGDIR);
      sb.append("/parts/DocOnly*;");
      // sb.append(" rm ");
      // sb.append(WORKINGDIR);
      // sb.append("/data/catTemp");
      String para = sb.toString();
      Process p = Runtime.getRuntime()
          .exec(new String[] { "bash", "-c", para });
      p.waitFor();
    } catch (Exception e) {
      System.out.println("exception happened - here's what I know: ");
      e.printStackTrace();
      System.exit(-1);
    }

    ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(
        indexFile));
    writer.writeObject(this);
    writer.close();
    processCatFile();
    System.out.println("Store index to: " + indexFile);
    // try to cat and sort
    File cat = new File(WORKINGDIR + "/data/catTemp");
    cat.delete();
    System.exit(0);
  }

  /*
   * Construct the index from all files under the directory
   */
  private void buildIndex(String directoryPath) {
    List<File> files = new ArrayList<File>();
    try {
      files = getFilesUnderDirectory(directoryPath);
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
    int fileSize = files.size();
    System.out.println("Total Document Size " + fileSize);

    for (int docId = 0; docId < fileSize; docId++) {
      buildOneDoc(files.get(docId), docId);
      if (docId != 0 && docId % 100 == 0) {
        // clear inverted map and write to file
        String fileName = "DocOnlyPartial" + String.valueOf(docId / 100);
        WriteToFile wr = new WriteToFile(fileName);
        wr.appendListToFile(invertedMap);
        invertedMap = new HashMap<Integer, List<Integer>>();
      }
      if (docId == fileSize - 1) {
        String fileName = "DocOnlyLast";
        WriteToFile wr = new WriteToFile(fileName);
        wr.appendListToFile(invertedMap);
        invertedMap = new HashMap<Integer, List<Integer>>();
      }
    }
  }

  private void buildOneDoc(File file, int docId) {
    String docTitle = null;
    String docBody = null;
    Stemmer stemmer = new Stemmer();
    Pattern pattern = Pattern.compile("\\s+");

    org.jsoup.nodes.Document doc = null;
    try {
      doc = Jsoup.parse(file, "UTF-8", "");
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
    docTitle = doc.title().toLowerCase();
    docBody = doc.text().toLowerCase();
    List<String> docTitleList = new ArrayList<String>();
    List<String> docBodyList = new ArrayList<String>();
    // parse title of the document
    Scanner scanner = new Scanner(docTitle).useDelimiter(pattern);
    while (scanner.hasNext()) {
      String beforeStemming = scanner.next();
      Vector<String> temp = Tokenization(beforeStemming);
      // if the String is ASCII encoded
      for (int i = 0; i < temp.size(); i++) {
        String s_temp = temp.get(i);
        if (CharMatcher.ASCII.matchesAllOf(s_temp)) {
          String afterStemming = stemmer.stemStringInstance(s_temp);
          // System.out.println("After Stemming " + afterStemming);
          docTitleList.add(afterStemming);
        } else {
          docTitleList.add(s_temp);
        }
      }
    }
    scanner.close();

    // parse body of the document
    scanner = new Scanner(docBody).useDelimiter(pattern);
    while (scanner.hasNext()) {
      String beforeStemming = scanner.next();
      Vector<String> temp = Tokenization(beforeStemming);
      // if the String is ASCII encoded
      for (int i = 0; i < temp.size(); i++) {
        String s_temp = temp.get(i);
        if (CharMatcher.ASCII.matchesAllOf(s_temp)) {
          String afterStemming = stemmer.stemStringInstance(s_temp);
          // System.out.println("After Stemming " + afterStemming);
          docTitleList.add(afterStemming);
        } else {
          docTitleList.add(s_temp);
        }
      }
    }
    scanner.close();

    // Here we are in the loop of reading one document
    DocumentIndexed document = new DocumentIndexed(docId);
    document.setTitle(docTitle);
    document.setUrl(file.getAbsolutePath());
    document.setBodyFrequency(docBodyList.size());
    urlToDoc.put(file.getAbsolutePath(), document._docid);
    _documents.add(document);
    Set<Integer> uniqueIds = new HashSet<Integer>();
    processList(docTitleList, docId, uniqueIds);
    processList(docBodyList, docId, uniqueIds);
    ++_numDocs;
    updateTermDocFreq(uniqueIds);
    processInvertedMap(uniqueIds, docId);

  }

  /*
   * update the inverted map
   */
  private void processInvertedMap(Set<Integer> uniques, int docId) {
    for (Integer i : uniques) {
      if (!invertedMap.containsKey(i)) {
        List<Integer> invertedList = new ArrayList<Integer>();
        invertedList.add(docId);
        invertedMap.put(i, invertedList);
      } else {
        List<Integer> invertedList = invertedMap.get(i);
        invertedList.add(docId);
        invertedMap.put(i, invertedList);
      }
    }
  }

  /*
   * update the index information
   */
  private void processList(List<String> contentList, int docid,
      Set<Integer> uniqueTerms) {
    for (String s : contentList) {
      int termId = -1;
      if (!_dictionary.keySet().contains(s)) {
        termId = _dictionary.keySet().size();
        // _terms.add(s);
        _dictionary.put(s, termId);
        _termCorpusFrequency.put(termId, 1);
        Map<Integer, Integer> documentTermMap = new HashMap<Integer, Integer>();
        documentTermMap.put(docid, 1);
        documentTermOccurencesMap.put(termId, documentTermMap);
        // System.out.println(documentTermOccurencesMap.size());
      } else {
        termId = _dictionary.get(s);
        _termCorpusFrequency.put(termId, _termCorpusFrequency.get(termId) + 1);
        Map<Integer, Integer> documentTermMap = documentTermOccurencesMap
            .get(termId);
        if (!documentTermMap.containsKey(docid)) {
          // first time the term occur in the document
          documentTermMap.put(docid, 1);
        } else {
          documentTermMap.put(docid, documentTermMap.get(docid) + 1);
        }
        documentTermOccurencesMap.put(termId, documentTermMap);
        // System.out.println(documentTermOccurencesMap.size());
      }
      _totalTermFrequency++;
      uniqueTerms.add(termId);
    }
  }

  private void updateTermDocFreq(Set<Integer> wordIndex) {
    for (Integer index : wordIndex) {
      if (_termDocFrequency.containsKey(index)) {
        _termDocFrequency.put(index, _termDocFrequency.get(index) + 1);
      } else {
        _termDocFrequency.put(index, 1);
      }
    }
  }

  /*
   * Return all files under a certain directory
   */
  private List<File> getFilesUnderDirectory(String directoryPath)
      throws IOException {
    File root = new File(directoryPath);
    List<File> files = new ArrayList<File>();
    if (!root.isDirectory()) {
      throw new IOException("The corpus path " + directoryPath
          + " is not a directory!");
    } else {
      File[] subfiles = root.listFiles();
      for (File f : subfiles) {
        if (!f.isDirectory()) {
          files.add(f);
        } else {
          files.addAll(getFilesUnderDirectory(f.getAbsolutePath()));
        }
      }
    }
    return files;
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/indexerInvertedDoconly.idx";
    indexFile = WORKINGDIR + "/" + indexFile;
    System.out.println("Load index from: " + indexFile);
    ObjectInputStream reader = new ObjectInputStream(new FileInputStream(
        indexFile));
    // long time = System.currentTimeMillis();
    IndexerInvertedDoconly loaded = (IndexerInvertedDoconly) reader
        .readObject();
    // System.out.println("Load Index takes " +
    // String.valueOf(System.currentTimeMillis()-time) + " milliseconds.");
    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    for (Integer freq : loaded._termCorpusFrequency.values()) {
      this._totalTermFrequency += freq;
    }
    this._dictionary = loaded._dictionary;
    // this._terms = loaded._terms;
    this._termCorpusFrequency = loaded._termCorpusFrequency;
    this._termDocFrequency = loaded._termDocFrequency;
    this.urlToDoc = loaded.urlToDoc;
    // this.invertedMap = loaded.invertedMap;
    // System.out.println(this.invertedMap.size()==0);
    this.documentTermOccurencesMap = loaded.documentTermOccurencesMap;
    this.invertedListIndex = loaded.invertedListIndex;
    // this.invertedByte = loaded.invertedByte;
    reader.close();

    System.out.println(Integer.toString(_numDocs) + " documents loaded "
        + "with " + Long.toString(_totalTermFrequency) + " terms!");
  }

  @Override
  public Document getDoc(int docid) {
    return _documents.get(docid);
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    if (query == null)
      return null;
    List<String> queryTokens = query._tokens;
    int[] docIds = new int[queryTokens.size()];
    for (int i = 0; i < queryTokens.size(); i++) {
      String token = queryTokens.get(i);
      int termId;
      if (!_dictionary.containsKey(token)) {
        termId = -1;
      } else {
        termId = _dictionary.get(token);
      }
      docIds[i] = next(termId, docid);
    }
    int maxDocId = docIds[0];
    boolean hit = true;
    for (int i = 0; i < docIds.length; i++) {
      if (docIds[i] == Integer.MAX_VALUE) {
        return null;
      }
      if (maxDocId != docIds[i])
        hit = false;
      maxDocId = Math.max(maxDocId, docIds[i]);
    }
    if (hit) {
      return _documents.get(maxDocId);
    } else {
      return nextDoc(query, maxDocId - 1);
    }
  }

  /**
   * Return a list all satisfied documents. Pay attention here we don't handle
   * phrase here
   * 
   * @param query
   * @return
   */
  public List<Document> getAllDocuments(Query query) {
    if (query == null)
      return null;
    int docId = -1;
    List<Document> hitDocuments = new ArrayList<Document>();
    Document d = null;
    while ((d = nextDoc(query, docId)) != null) {
      hitDocuments.add(_documents.get(d._docid));
      docId = d._docid;
    }
    return hitDocuments;
  }

  private void fetchInvertedMapFromFile(Integer termId) {
    String filePath = WORKINGDIR + "/data/index/final1";
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(filePath));
      String line = null;
      while ((line = br.readLine()) != null) {
        String[] oneLine = line.split(" ");
        if (oneLine == null || oneLine.length < 3)
          continue;
        int getTermId = Integer.parseInt(oneLine[0]);
        if (getTermId == termId.intValue()) {
          // get one list; should add the list to the inverted map
          int listSize = Integer.parseInt(oneLine[1]);
          List<Integer> postingList = new ArrayList<Integer>();
          for (int i = 0; i < listSize; i++) {
            postingList.add(Integer.parseInt(oneLine[i + 2]));
          }
          // finish one line reading
          invertedMap.put(termId, postingList);
        } else if (getTermId > termId) { // has get all the invertList of the
                                         // termID
          break;
        }
      }
      // finally sort the posting list
      List<Integer> list = invertedMap.get(termId);
      Collections.sort(list);
      invertedMap.put(termId, list);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }

  private void processCatFile() {
    BufferedReader br = null;
    BufferedWriter bw = null;

    String catToFile = WORKINGDIR + "/data/index/final1";
    String catFile = WORKINGDIR + "/data/catTemp";
    File outputFile;
    try {
      outputFile = new File(catToFile);
      if (!outputFile.exists()) {
        outputFile.createNewFile();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      br = new BufferedReader(new FileReader(catFile));
      bw = new BufferedWriter(new FileWriter(catToFile));
      int lastLineTermId = 0;
      String line = null;
      List<Integer> postingList = new ArrayList<Integer>();
      while ((line = br.readLine()) != null) { // every line:add to map (term ->
                                               // map (doc -> list of positions)
                                               // )
        String[] thisLine = line.split(" ");
        if (thisLine == null || thisLine.length < 3)
          continue;
        int term_id = Integer.parseInt(thisLine[0]); // term id
        if (lastLineTermId == term_id) {
          int listSize = Integer.parseInt(thisLine[1]);
          for (int i = 0; i < listSize; i++) {
            postingList.add(Integer.valueOf(thisLine[i + 2]));
          }
        } else {
          StringBuilder sb = new StringBuilder();
          sb.append(lastLineTermId);
          sb.append(" ");
          sb.append(postingList.size());
          sb.append(" ");
          Collections.sort(postingList);
          for (int i = 0; i < postingList.size(); i++) {
            sb.append(postingList.get(i));
            if (i == postingList.size() - 1) {
              sb.append('\n');
            } else {
              sb.append(' ');
            }
          }
          bw.write(sb.toString());
          bw.flush();
          // write last line to output
          // add this line
          postingList = new ArrayList<Integer>();
          int listSize = Integer.parseInt(thisLine[1]);
          for (int i = 0; i < listSize; i++) {
            postingList.add(Integer.valueOf(thisLine[i + 2]));
          }
          lastLineTermId = term_id;
        }
      }
      StringBuilder sb = new StringBuilder();
      sb.append(lastLineTermId);
      sb.append(' ');
      sb.append(postingList.size());
      sb.append(' ');
      Collections.sort(postingList);
      for (int i = 0; i < postingList.size(); i++) {
        sb.append(postingList.get(i));
        if (i == postingList.size() - 1) {
          sb.append('\n');
        } else {
          sb.append(' ');
        }
      }
      bw.write(sb.toString());
      bw.flush();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (br != null) {
          br.close();
        }
        if (bw != null) {
          bw.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /*
   * change access type from private to default for testing purpose
   */
  int next(Integer termId, int currentDocId) {
    // the term does not appear in the corpus
    if (termId == -1) {
      return Integer.MAX_VALUE;
    }
    fetchInvertedMapFromFile(termId);
    // String term = _terms.get(termId);
    // int termId = _dictionary.get(term);
    if (!invertedListIndex.containsKey(termId)) {
      invertedListIndex.put(termId, 0);
    }
    int offset = invertedListIndex.get(termId);
    int low = offset;
    List<Integer> postingList = invertedMap.get(termId);
    int postingListSize = postingList.size();
    // the end of the posting list is smaller than or equal to the currentDocId
    if (postingList.get(postingListSize - 1) <= currentDocId) {
      return Integer.MAX_VALUE;
    }
    // the beginning of the posting list is larger than the current document Id
    // thus update the offset to 0 and return the first element in the posting
    // list
    if (postingList.get(0) > currentDocId) {
      invertedListIndex.put(termId, 0);
      return postingList.get(0);
    }
    // update the search beginning lower bound
    // low represents the lowest possible index
    if (offset > 0 && postingList.get(offset) <= currentDocId) {
      low = offset + 1;
    } else {
      low = 0;
    }

    // high represents the highest possible position
    int jump = 1;
    int high = low + jump;
    while (high < postingListSize && postingList.get(high) <= currentDocId) {
      low = high + 1;
      jump *= 2;
      high = low + jump;
    }
    if (high >= postingListSize) {
      high = postingListSize - 1;
    }
    offset = binarySearch(postingList, low, high, currentDocId);
    invertedListIndex.put(termId, offset);
    return postingList.get(offset);
  }

  /*
   * low and high are inclusive and return the possible position of the list
   * whose value is larger and mostly close to the value
   */
  private int binarySearch(List<Integer> postingList, int low, int high,
      int nextLargerThanThis) {
    int mid;
    // exit the loop when low == high
    while (low < high) {
      mid = (low + high) / 2;
      if (postingList.get(mid) == nextLargerThanThis) {
        return mid + 1;
      }
      if (postingList.get(mid) < nextLargerThanThis) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return (low + high) / 2;
  }

  // Number of documents in which {@code term} appeared, over the full corpus.
  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return _dictionary.containsKey(term) ? _termDocFrequency.get(_dictionary
        .get(term)) : 0;
  }

  // Number of times {@code term} appeared in corpus.
  @Override
  public int corpusTermFrequency(String term) {
    return _dictionary.containsKey(term) ? _termCorpusFrequency.get(_dictionary
        .get(term)) : 0;
  }

  // Number of times {@code term} appeared in the document {@code url}.
  
  public int documentTermFrequency(String term, String url) {
    if (!urlToDoc.containsKey(url)) {
      return 0;
    }
    int docId = urlToDoc.get(url);
    if (!_dictionary.containsKey(term) || docId == -1)
      return 0;
    int termId = _dictionary.get(term);
    if (invertedMap.containsKey(termId)) {
      if (invertedMap.get(termId).contains(docId)) {
        return 1;
      }
    }
    return 0;
    // Map<Integer, Integer> documentTermOccur = documentTermOccurencesMap
    // .get(termId);
    // return documentTermOccur.get(docId);
  }

  @Override
  public double getDocumentSize(int docId) {
    if (docId >= _documents.size()) {
      return -1;
    }
    return ((DocumentIndexed) _documents.get(docId)).getBodyFrequency();
  }

  public void buildInvertMap(Query query) {
    if (query == null)
      return;
    Vector<String> tokens = query._tokens;
    for (String s : tokens) {
      int termId = _dictionary.get(s);
      fetchInvertedMapFromFile(termId);
    }
  }
  @Override
  public int documentTermFrequency(String term, int docid) {
    SearchEngine.Check(false, "Not implemented!");
    return 0;
  }
}
