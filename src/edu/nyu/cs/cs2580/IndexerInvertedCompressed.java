package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;

import com.google.common.base.CharMatcher;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer implements Serializable {

  private static final long serialVersionUID = 4574090044401455221L;

  private final String WORKINGDIR = System.getProperty("user.dir");
  private final String PARTS = "/parts/";
  private final int PROCESSUNIT = 500;
  private final String FINALINDEX = "/data/index/final3.idx";

  private long offsetForTerm = 0L;

  private transient RandomAccessFile finalPostingLists = null;

  // Maps each term to their integer representation
  Map<String, Integer> _dictionary = new HashMap<String, Integer>();

  // Term document frequency, key is the integer representation of the term and
  // value is the number of documents the term appears in.
  private Map<Integer, Integer> _termDocFrequency = new HashMap<Integer, Integer>();
  // Term frequency, key is the integer representation of the term and value is
  // the number of times the term appears in the corpus.
  private Map<Integer, Integer> _termCorpusFrequency = new HashMap<Integer, Integer>();

  // Stores all Document in memory.
  List<Document> _documents = new ArrayList<Document>();

  // In-Memory Indexer: Key is the integer representation of the term and value
  // is the a HashMap from docId to each position with the term appears in the
  // docId; compress the occurence list with V-byte encoding
  transient Map<Integer, Map<Integer, List<Integer>>> invertedMap = new HashMap<Integer, Map<Integer, List<Integer>>>();

  // map each url string to the document id
  Map<String, Integer> urlToDocId = new HashMap<String, Integer>();

  // key is the value representation of the term and value is offset of the
  // term in the byte array
  Map<Integer, Long> offsetInByteArray = new HashMap<Integer, Long>();

  // Key is the integer representation of the term and value is the cached
  // offset of the posting list
  transient Map<Integer, Integer> invertedListIndex = new HashMap<Integer, Integer>();

  private transient List<String> tempFileName = new ArrayList<String>();

  private transient BufferedReader[] readingFiles = null;

  private transient String[] catchedLines = null;

  private Map<Integer, Float> prResult = new HashMap<Integer, Float>();
  private Map<Integer, Integer> numResult = new HashMap<Integer, Integer>();

  public IndexerInvertedCompressed() {
  }

  public IndexerInvertedCompressed(Options options) {
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
    System.out.println("Construct index from: " + corpusFile);
    long beginTime = System.currentTimeMillis();
    buildIndex(corpusFile);
    System.out.println("Indexed " + Integer.toString(_numDocs) + " docs with "
        + Long.toString(_totalTermFrequency) + " terms.");
    String indexFile = _options._indexPrefix + "/indexInvertedCompressed.idx";
    // write to file
    buildWholeIndexFromPartial();
    loadPageRankValue();
    loadNumView();
    ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(
        indexFile));
    writer.writeObject(this);
    writer.close();
    System.out.println("Store index to: " + indexFile);
    System.out.println("Store posting list to " + FINALINDEX);
    long now = System.currentTimeMillis();
    System.out.println("In total: " + (now - beginTime) / 1000 + " seconds.");
  }

  @Override
  public void loadIndex() throws IOException, ClassNotFoundException {
    String indexFile = _options._indexPrefix + "/indexInvertedCompressed.idx";
    System.out.println("Load index from: " + indexFile);

    ObjectInputStream reader = new ObjectInputStream(new FileInputStream(
        indexFile));
    IndexerInvertedCompressed loaded = (IndexerInvertedCompressed) reader
        .readObject();

    this._documents = loaded._documents;
    // Compute numDocs and totalTermFrequency b/c Indexer is not serializable.
    this._numDocs = _documents.size();
    for (Integer freq : loaded._termCorpusFrequency.values()) {
      this._totalTermFrequency += freq;
    }
    this._dictionary = loaded._dictionary;
    this._termCorpusFrequency = loaded._termCorpusFrequency;
    this._termDocFrequency = loaded._termDocFrequency;
    this.urlToDocId = loaded.urlToDocId;
    this.offsetInByteArray = loaded.offsetInByteArray;
    this.prResult = loaded.prResult;
    this.numResult = loaded.numResult;
    reader.close();
    System.out.println(Integer.toString(_numDocs) + " documents loaded "
        + "with " + Long.toString(_totalTermFrequency) + " terms!");
  }

  // read all data from the partial file and put that back into the final file
  private void buildWholeIndexFromPartial() {
    try {
      constructBufferedReader();
      int allTermsSize = _dictionary.keySet().size();
      for (int i = 0; i < allTermsSize; i += PROCESSUNIT) {
        TreeMap<Integer, Map<Integer, List<Integer>>> tempInvertMap = new TreeMap<Integer, Map<Integer, List<Integer>>>();
        for (int j = 0; j < tempFileName.size(); j++) {
          loadFromTermIdToTermIdtoMemory(i, i + PROCESSUNIT - 1, j,
              tempInvertMap);
        }
        // now write each term with its compressed version to the file
        for (Map.Entry<Integer, Map<Integer, List<Integer>>> entry : tempInvertMap
            .entrySet()) {
          byte[] compressedByte = Compress.EncodeOneTermMap(entry.getKey(),
              entry.getValue());
          offsetInByteArray.put(entry.getKey(), offsetForTerm);
          finalPostingLists.write(compressedByte);
          offsetForTerm = offsetForTerm + compressedByte.length;
        }
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (IllegalStateException e) {
      e.printStackTrace();
    } finally {
      for (BufferedReader reader : readingFiles) {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
      if (finalPostingLists != null) {
        try {
          finalPostingLists.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
        finalPostingLists = null;
      }
    }
    // delete all the partial file
    for (String partialFilePath : tempFileName) {
      File file = new File(partialFilePath);
      if (file.exists()) {
        file.delete();
      }
    }
  }

  private void constructBufferedReader() throws IOException {
    int fileSize = tempFileName.size();
    readingFiles = new BufferedReader[fileSize];
    catchedLines = new String[fileSize];
    for (int i = 0; i < fileSize; i++) {
      String filePath = tempFileName.get(i);
      BufferedReader reader = new BufferedReader(new FileReader(filePath));
      readingFiles[i] = reader;
      catchedLines[i] = null;
    }
    String finalFilePath = WORKINGDIR + FINALINDEX;
    File file = new File(finalFilePath);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    finalPostingLists = new RandomAccessFile(file, "rw");
  }

  // load the id range from all the parial file into the memory
  // the keyset size thus is under control here so we could use treemap here
  // if beginId = 100 endingId = 299; load term100 - load299 in memory
  // pass in the parial file Id
  private void loadFromTermIdToTermIdtoMemory(int beginId, int endingId,
      int readerId, TreeMap<Integer, Map<Integer, List<Integer>>> tempInvertMap)
      throws IllegalStateException, IOException {
    BufferedReader reader = readingFiles[readerId];
    // the bufferedreader is not initiated or closed when meet the end of the
    // file
    if (reader == null)
      return;
    String catchedLine = catchedLines[readerId];
    if (catchedLine != null && catchedLine.length() != 0) {
      String[] lineContent = catchedLine.split(" ", 2);
      // the cached line is still larger than the ending line
      if (Integer.parseInt(lineContent[0]) > endingId) {
        return;
      } else {
        // add this line to the map and determine whether it equals to the
        // ending Id
        Map<Integer, List<Integer>> valueMap = buildMapList(catchedLine);
        int termId = Integer.parseInt(lineContent[0]);
        if (!tempInvertMap.containsKey(termId)) {
          tempInvertMap.put(termId, valueMap);
        } else {
          Map<Integer, List<Integer>> temp = tempInvertMap.get(termId);
          for (Map.Entry<Integer, List<Integer>> entry : valueMap.entrySet()) {
            // this doc has occurence list added before
            if (temp.containsKey(entry.getKey())) {
              throw new IllegalStateException("The document " + entry.getKey()
                  + " for term " + termId + " has been processed");
            }
            temp.put(entry.getKey(), entry.getValue());
          }
        }
        // happens to be the last line
        if (termId == endingId) {
          return;
        }
      }
    }
    // the line is empty or we have processed the cachedLine and need to read
    // next line
    while ((catchedLine = reader.readLine()) != null) {
      String[] lineContent = catchedLine.split(" ", 2);
      if (Integer.valueOf(lineContent[0]) > endingId) {
        catchedLines[readerId] = catchedLine;
        return;
      } else {
        // process each line here
        Map<Integer, List<Integer>> valueMap = buildMapList(catchedLine);
        int termId = Integer.parseInt(lineContent[0]);
        if (!tempInvertMap.containsKey(termId)) {
          tempInvertMap.put(termId, valueMap);
        } else {
          Map<Integer, List<Integer>> temp = tempInvertMap.get(termId);
          for (Map.Entry<Integer, List<Integer>> entry : valueMap.entrySet()) {
            // this doc has occurence list added before
            if (temp.containsKey(entry.getKey())) {
              throw new IllegalStateException("The document " + entry.getKey()
                  + " for term " + termId + " has been processed");
            }
            temp.put(entry.getKey(), entry.getValue());
          }
        }
      }
    }
    // end of the file
    if (catchedLine == null) {
      reader.close();
      readingFiles[readerId] = null;
    }
  }

  // index 100 docs into one document and make sure the term is in sorted order
  private void buildIndex(String directoryPath) {
    List<File> files = new ArrayList<File>();
    try {
      files = getFilesUnderDirectory(directoryPath);
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
    int files_size = files.size();
    System.out.println("Total Document Size " + files_size);

    for (int docId = 0; docId < files_size; docId++) {
      buildOneDoc(files.get(docId), docId);
      if (docId != 0 && docId % 100 == 0) {
        // clear inverted map and write to file
        String fileName = WORKINGDIR + PARTS + "PartialIndexCompressed"
            + String.valueOf(docId / 100);
        tempFileName.add(fileName);
        WriteToFile wr = new WriteToFile(fileName);
        // sort the map based on termId
        // no need to sort the inner map based on docId and write each term to
        // file
        List<Integer> termIdList = new ArrayList<Integer>(invertedMap.keySet());
        Collections.sort(termIdList);
        for (int i = 0; i < termIdList.size(); i++) {
          int termId = termIdList.get(i);
          Map<Integer, List<Integer>> innerDocToOccMap = invertedMap
              .get(termId);
          wr.appendOneTermMapToFile(termId, innerDocToOccMap);
        }
        // have write all the map to the file and now clear the memory
        invertedMap = new HashMap<Integer, Map<Integer, List<Integer>>>();
        wr.closeBufferWriter();
      } else if (docId == files_size - 1) {
        String fileName = WORKINGDIR + PARTS + "PartialIndexCompressedLast";
        tempFileName.add(fileName);
        WriteToFile wr = new WriteToFile(fileName);
        List<Integer> termIdList = new ArrayList<Integer>(invertedMap.keySet());
        Collections.sort(termIdList);
        for (int i = 0; i < termIdList.size(); i++) {
          int termId = termIdList.get(i);
          Map<Integer, List<Integer>> innerDocToOccMap = invertedMap
              .get(termId);
          wr.appendOneTermMapToFile(termId, innerDocToOccMap);
        }
        // have write all the map to the file and now clear the memory
        invertedMap = new HashMap<Integer, Map<Integer, List<Integer>>>();
        wr.closeBufferWriter();
      }
    }
  }

  private void loadPageRankValue() {
    CorpusAnalyzerPagerank cpr = new CorpusAnalyzerPagerank(_options);
    Map<String, Float> pr = new HashMap<String, Float>();
    try {
      pr = cpr.load();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (pr.size() == 0) {
      throw new IllegalStateException("The index load page rank value error");
    }
    for (Map.Entry<String, Float> entry : pr.entrySet()) {
      if (!urlToDocId.containsKey(entry.getKey())) {
        try {
          throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
          e.printStackTrace();
        }
      }
      int docId = urlToDocId.get(entry.getKey());
      prResult.put(docId, entry.getValue());
    }
  }

  private void loadNumView() {
    LogMinerNumviews loger = new LogMinerNumviews(_options);
    Map<String, Integer> num = new HashMap<String, Integer>();
    try {
      num = loger.load();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (num.size() == 0) {
      throw new IllegalStateException("The index load num view error");
    }
    for (Map.Entry<String, Integer> entry : num.entrySet()) {
      if (!urlToDocId.containsKey(entry.getKey())) {
        try {
          throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
          e.printStackTrace();
        }
      }
      int docId = urlToDocId.get(entry.getKey());
      numResult.put(docId, entry.getValue());
    }
  }

  /*
   * Construct the index from all files under the directory
   */
  private void buildOneDoc(File file, int docId) {

    String docTitle = null;
    String docBody = null;
    Stemmer stemmer = new Stemmer();
    Pattern pattern = Pattern.compile("\\s+");

    org.jsoup.nodes.Document doc = null;

    try {
      doc = Jsoup.parse(file, "UTF-8", "");
    } catch (IOException e) {
      e.printStackTrace();
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

      for (int i = 0; i < temp.size(); i++) {
        String s_temp = temp.get(i);
        if (CharMatcher.ASCII.matchesAllOf(s_temp)) {
          String afterStemming = stemmer.stemStringInstance(s_temp);
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

      for (int i = 0; i < temp.size(); i++) {
        String s_temp = temp.get(i);
        if (CharMatcher.ASCII.matchesAllOf(beforeStemming)) {
          String afterStemming = stemmer.stemStringInstance(beforeStemming);
          docBodyList.add(afterStemming);
        } else {
          docBodyList.add(String.valueOf(s_temp));
        }
      }
    }
    scanner.close();

    // Put the information that the ranker need to know about the document
    DocumentIndexed document = new DocumentIndexed(docId);
    document.setTitle(docTitle);
    document.setUrl(URIParser.parseFileNameToUTF8(file.getName()));
    urlToDocId.put(URIParser.parseFileNameToUTF8(file.getName()),
        document._docid);
    document.setBodyFrequency(docBodyList.size());
    _documents.add(document);

    Set<Integer> uniqueIds = new HashSet<Integer>();
    List<Integer> titleIntegerList = processList(docTitleList, docId, uniqueIds);
    List<Integer> bodyIntegerList = processList(docBodyList, docId, uniqueIds);
    ++_numDocs;
    updateTermDocFreq(uniqueIds);
    // update the inverted map only based on the body now
    processInvertedMap(docId, bodyIntegerList);
  }

  /*
   * update the inverted map
   */
  private void processInvertedMap(int docId, List<Integer> docContentList) {
    // docContentList could be doc title or doc body
    int docContentList_len = docContentList.size();
    for (int i = 0; i < docContentList_len; i++) {
      int termId = docContentList.get(i);
      Map<Integer, List<Integer>> doc_pos = new HashMap<Integer, List<Integer>>();
      List<Integer> pos_list = new ArrayList<Integer>();
      if (invertedMap.containsKey(termId)) {
        doc_pos = invertedMap.get(termId);
        if (doc_pos.containsKey(docId)) {
          pos_list = doc_pos.get(docId);
          pos_list.add(i);
          doc_pos.put(docId, pos_list);
        } else {
          pos_list.add(i);
          doc_pos.put(docId, pos_list); // put is in fact 'add'
        }
        invertedMap.put(termId, doc_pos);
      } else { // new term for invertedMap
        pos_list.add(i);
        doc_pos.put(docId, pos_list);
        invertedMap.put(termId, doc_pos);
      }
    }
  }

  /*
   * update the _dictionary, _terms, _termCorpusFrequency and
   * _totalTermFrequency return a list of integer representation of the term
   */
  private List<Integer> processList(List<String> contentList, int docid,
      Set<Integer> uniqueTerms) {
    List<Integer> results = new ArrayList<Integer>();
    for (String s : contentList) {
      int termId = -1;
      if (!_dictionary.keySet().contains(s)) {
        termId = _dictionary.keySet().size();
        // _terms.add(s);
        _dictionary.put(s, termId);
        _termCorpusFrequency.put(termId, 1);
      } else {
        termId = _dictionary.get(s);
        _termCorpusFrequency.put(termId, _termCorpusFrequency.get(termId) + 1);
      }
      _totalTermFrequency++;
      uniqueTerms.add(termId);
      results.add(termId);
    }
    return results;
  }

  // Term document frequency, key is the integer representation of the term and
  // value is the number of documents the term appears in.
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

  // based on the string to build term map
  private Map<Integer, List<Integer>> buildMapList(String input) {
    Map<Integer, List<Integer>> r = new HashMap<Integer, List<Integer>>();
    Scanner s = new Scanner(input);
    int key = Integer.parseInt(s.next()); // this term id
    int v_num = Integer.parseInt(s.next()); // the number of docs for this term
    for (int i = 0; i < v_num; i++) {
      int temp_key = Integer.parseInt(s.next()); // doc id
      int temp_num = Integer.parseInt(s.next()); // num of positions
      List<Integer> num_list = new ArrayList<Integer>();
      for (int j = 0; j < temp_num; j++) {
        num_list.add(Integer.parseInt(s.next()));
      }
      r.put(temp_key, num_list);
    }
    s.close();
    return r;
  }

  @Override
  public int corpusDocFrequencyByTerm(String term) {
    return _dictionary.containsKey(term) ? _termDocFrequency.get(_dictionary
        .get(term)) : 0;
  }

  @Override
  public int corpusTermFrequency(String term) {
    return _dictionary.containsKey(term) ? _termCorpusFrequency.get(_dictionary
        .get(term)) : 0;
  }

  public int documentTermFrequency(String term, String url) {
    if (!_dictionary.containsKey(term))
      return 0;
    if (!urlToDocId.containsKey(url)) {
      return 0;
    }
    int docId = urlToDocId.get(url);
    int termId = _dictionary.get(term);
    if (!invertedMap.get(termId).containsKey(docId)) {
      return 0;
    }
    return invertedMap.get(termId).get(docId).size();
  }

  @Override
  public double getDocumentSize(int docId) {
    if (docId >= _documents.size()) {
      return -1;
    }
    return ((DocumentIndexed) _documents.get(docId)).getBodyFrequency();
  }

  @Override
  public Document getDoc(int docid) {
    return _documents.get(docid);
  }

  /**
   * Return a list all satisfied documents. Pay attention here we need to handle
   * phrase here
   * 
   * @param query
   * @return
   */
  @Override
  public List<Document> getAllDocuments(Query query) {
    if (query == null)
      return null;
    QueryPhrase reviseQuery = new QueryPhrase(query._query);
    return getAllDocuments(reviseQuery);
  }

  private List<Document> getAllDocuments(QueryPhrase query) {
    int docId = -1;
    List<Document> hitDocuments = new ArrayList<Document>();
    Document d = null;
    while ((d = nextDoc(query, docId)) != null) {
      hitDocuments.add(_documents.get(d._docid));
      docId = d._docid;
    }
    return hitDocuments;
  }

  int next(Integer termId, int currentDocId) {
    // the term does not appear in the corpus
    if (termId == -1) {
      return Integer.MAX_VALUE;
    }
    // String term = _terms.get(termId);
    // int termId = _dictionary.get(term);
    if (!invertedListIndex.containsKey(termId)) { // new term
      invertedListIndex.put(termId, 0);
    }
    int offset = invertedListIndex.get(termId); // cached offset
    int low = offset;
    List<Integer> postingList = new ArrayList<Integer>(invertedMap.get(termId)
        .keySet());
    Collections.sort(postingList);
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

  public void buildInvertMap(Query query) {
    if (query == null)
      return;
    QueryPhrase qp = new QueryPhrase(query._query);
    String fileName = WORKINGDIR + FINALINDEX;
    Vector<String> queryTokens = query._tokens;
    Vector<Vector<String>> phrases = qp._phrases;
    try {
      buildMap(queryTokens, phrases, fileName);
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (Vector<String> phrase : phrases) {
      processOnePhrase(phrase);
    }
  }

  private void buildMap(Vector<String> tokens, Vector<Vector<String>> phrases,
      String location) throws IOException {

    Set<Integer> term_id = new HashSet<Integer>();
    Map<Integer, Map<Integer, List<Integer>>> r = new HashMap<Integer, Map<Integer, List<Integer>>>();
    List<String> terms = new ArrayList<String>();
    terms.addAll(tokens);
    if (phrases != null && phrases.size() != 0) {
      for (Vector<String> v : phrases) {
        terms.addAll(v);
      }
    }

    for (String term : terms) {
      if (_dictionary.containsKey(term)) {
        int tid = _dictionary.get(term);
        term_id.add(tid);
      }
    }

    finalPostingLists = new RandomAccessFile(WORKINGDIR + FINALINDEX, "r");
    for (Integer term : term_id) {
      if (!offsetInByteArray.containsKey(term)) {
        throw new IllegalStateException("The term " + term
            + " is not contained in the offset table");
      }
      long offset = offsetInByteArray.get(term);
      finalPostingLists.seek(offset);
      // the last term: read to end of the file
      int byteArraySize = (int) (offsetInByteArray.get(term + 1) - offset);
      byte[] compressedMap = new byte[byteArraySize];
      if ((finalPostingLists.read(compressedMap)) != -1) {
        r = Compress.DecodeOneTermMap(compressedMap, 0);
      }
      // update to the invertedMap
      if (!invertedMap.containsKey(term)) {
        invertedMap.put(term, r.get(term));
      }
    }
    finalPostingLists.close();
  }

  // to handle one phrase, possibly make the phrase a 'term'
  // TODOTODOTODOTODO
  void processOnePhrase(Vector<String> phrase) {
    int len = phrase.size(); // number of tokens in the phrase
    if (len == 0)
      return; // empty
    // if any token of the phrase not in _dictionary, discard the phrase
    for (int i = 0; i < len; i++) {
      if (!_dictionary.containsKey(phrase.get(i))) {
        return;
      }
    }

    if (len == 1) {
      // do nothing, it is already in.
      return;
    } else {
      String phrase_as_string = phrase.get(0);
      for (int count = 1; count < len; count++) {
        phrase_as_string += " ";
        phrase_as_string += phrase.get(count);
      }
      int all_term_occurrence = 0;
      int docId = -1;
      List<Document> hitDocuments = new ArrayList<Document>();
      Document d = null;
      while ((d = nextDoc(phrase, docId)) != null) {
        hitDocuments.add(_documents.get(d._docid));
        docId = d._docid;
      }
      List<Integer> hitDocIds = new ArrayList<Integer>();
      for (Document doc : hitDocuments) {
        hitDocIds.add(doc._docid);
      }
      Map<Integer, List<Integer>> doc_post_list = new HashMap<Integer, List<Integer>>();
      List<Integer> doc_list = new ArrayList<Integer>();
      for (Integer i : hitDocIds) { // for a specified hit doc
        List<Integer> pos_list_to_add = new ArrayList<Integer>();
        List<Integer> pos_list_0 = invertedMap.get(
            _dictionary.get(phrase.get(0))).get(i);
        for (int j : pos_list_0) {
          boolean flag = true;
          for (int k = 1; k < len; k++) {
            if (invertedMap.get(_dictionary.get(phrase.get(k))).get(i)
                .contains(j + k)) {

            } else {
              flag = false;
              break;
            }
          }
          if (flag == true) {
            // add position (j) to pos_list_to_add,
            pos_list_to_add.add(j);
            // add the phrase to _dictionary
            if (!_dictionary.containsKey(phrase_as_string)) {
              _dictionary.put(phrase_as_string, _dictionary.keySet().size());
              // _terms.add(phrase_as_string);
            }
          }
        }
        // map doc (id as i) and pos_list_to_add,
        if (pos_list_to_add.size() == 0) {
        } else {
          doc_post_list.put(i, pos_list_to_add);
          doc_list.add(i);
          all_term_occurrence += pos_list_to_add.size();
        }
      }
      if (_dictionary.containsKey(phrase_as_string)) {
        // map term and doc id
        invertedMap.put(_dictionary.get(phrase_as_string), doc_post_list);
        // update _termDocFrequency
        _termDocFrequency.put(_dictionary.get(phrase_as_string),
            doc_list.size());
        _termCorpusFrequency.put(_dictionary.get(phrase_as_string),
            all_term_occurrence);
      }
    }
  }

  // nextDoc, parameter as list of tokens, docid
  // helper method only for processOnePhrase
  private Document nextDoc(Vector<String> tokens, int docid) {
    if (tokens == null)
      return null;
    int[] docIds = new int[tokens.size()];
    for (int i = 0; i < tokens.size(); i++) {
      String token = tokens.get(i);
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
      return nextDoc(tokens, maxDocId - 1);
    }
  }

  /**
   * In HW2, you should be using {@link DocumentIndexed}.
   */
  @Override
  public Document nextDoc(Query query, int docid) {
    if (query == null)
      return null;
    QueryPhrase reviseQuery = new QueryPhrase(query._query);
    // buildInvertMap(reviseQuery);
    Vector<String> queryTokens = reviseQuery._tokens;
    Vector<Vector<String>> phrases = reviseQuery._phrases;
    for (Vector<String> p : phrases) {
      String phrase_as_string = p.get(0);
      for (int count = 1; count < p.size(); count++) {
        phrase_as_string += " ";
        phrase_as_string += p.get(count);
      }
      if (_dictionary.containsKey(phrase_as_string)) {
        queryTokens.add(phrase_as_string);
      } else {
        System.err.println("The phrase should be processed before!");
      }
    }
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

  void decodeInMemoryMap(byte[] input) {
    for (Integer termId : offsetInByteArray.keySet()) {
      long offset = offsetInByteArray.get(termId);
      Map<Integer, Map<Integer, List<Integer>>> partialMap = Compress
          .DecodeOneTermMap(input, offset);
      if (invertedMap.containsKey(termId)) {
        throw new IllegalStateException("This term has been processed");
      } else {
        invertedMap.put(termId, partialMap.get(termId));
      }
    }
  }

  /**
   * @CS2580: Implement this to work with your RankerFavorite.
   */
  @Override
  public int documentTermFrequency(String term, int docid) {
    if (!_dictionary.containsKey(term))
      return 0;
    if (docid >= _documents.size()) {
      return 0;
    }
    int termId = _dictionary.get(term);
    if (!invertedMap.get(termId).containsKey(docid)) {
      return 0;
    }
    return invertedMap.get(termId).get(docid).size();
  }
}
