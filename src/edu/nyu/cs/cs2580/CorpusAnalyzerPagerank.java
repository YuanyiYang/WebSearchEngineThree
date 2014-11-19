package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * Calculate the PageRank for each document
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {

  private float gamma; // 0.1 0.9
  private int iteration; // 1 2
  private final String WORKINGDIR = System.getProperty("user.dir");
  // PageRank result
  private String PR_FILE = null; //WORKINGDIR + "/data/index/prResult" ;
  //private final String CORPUS_LOC = WORKINGDIR + "/data/wiki";
  private final String PARTIAL_PRFILE = WORKINGDIR + "/parts/PartialPRGraph";
  
  // file name(URI) --> docId
  public Map<String, Integer> pageIndex = new LinkedHashMap<String, Integer>();

  // _prGraph.get(i) ==> map<j,numLink> represents how many link(numLink)
  // document j has that points to document i
  public Map<Integer, Map<Integer, Integer>> _prGraph 
           = new HashMap<Integer, Map<Integer, Integer>>();

  // num = outLink.get(i) represent how many outgoinglink i has
  public Map<Integer, Integer> _outLinks = new HashMap<Integer, Integer>();

  public List<Float> pr = new ArrayList<Float>();

  public CorpusAnalyzerPagerank(Options options) {
    super(options);
    setRunEnv(1, 0.9f);
  }

  /*
   * Only used for test purpose
   */
  public CorpusAnalyzerPagerank() {
    setRunEnv(1, 0.9f);
  }

  // set the text file location, iteration times, gamma value
  public void setRunEnv(int iteration, float gamma) {
    this.gamma = gamma;
    this.iteration = iteration;
    PR_FILE = WORKINGDIR + "/data/index/prResult" + String.valueOf(iteration) + String.valueOf(gamma);
  }

  /**
   * This function processes the corpus as specified inside {@link _options} and
   * extracts the "internal" graph structure from the pages inside the corpus.
   * Internal means we only store links between two pages that are both inside
   * the corpus.
   * 
   * Note that you will not be implementing a real crawler. Instead, the corpus
   * you are processing can be simply read from the disk. All you need to do is
   * reading the files one by one, parsing them, extracting the links for them,
   * and computing the graph composed of all and only links that connect two
   * pages that are both in the corpus.
   * 
   * Note that you will need to design the data structure for storing the
   * resulting graph, which will be used by the {@link compute} function. Since
   * the graph may be large, it may be necessary to store partial graphs to disk
   * before producing the final graph.
   * 
   * @throws IOException
   */
  @Override
  public void prepare() throws IOException {
    System.out.println("Preparing " + this.getClass().getName());
    List<File> files = new ArrayList<File>();
    String CORPUS_LOC = _options._corpusPrefix;
    try {
      files = getFilesUnderDirectory(CORPUS_LOC);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
    File partialFile = new File(PARTIAL_PRFILE);
    if (partialFile.exists()) {
      partialFile.delete();
    }
    WriteToFile writer = new WriteToFile(PARTIAL_PRFILE);
    for (int docId = 0; docId < files.size(); docId++) {
      if (!isValidDocument(files.get(docId))) {
        continue;
      }
      buildOneDoc(files.get(docId), docId, writer);
    }
    writer.closeBufferWriter();
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(PARTIAL_PRFILE));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    String docURI = null;
    while ((docURI = reader.readLine()) != null) {
      int j = pageIndex.get(docURI);
      int validOutLinkNum = 0;
      int outLinks = Integer.parseInt(reader.readLine());
      while (outLinks > 0) {
        String outLink = reader.readLine();
        outLinks--;
        if (!pageIndex.keySet().contains(outLink)) {
          continue;
        }
        int i = pageIndex.get(outLink);
        validOutLinkNum++;
        // docURI -> outLink _prGraph.get(i) ==> map<j,numLink> j points to i
        Map<Integer, Integer> matrix = null;
        if (!_prGraph.containsKey(i)) {
          matrix = new HashMap<Integer, Integer>();
        } else {
          matrix = _prGraph.get(i);
        }
        if (matrix.containsKey(j)) {
          matrix.put(j, matrix.get(j) + 1);
        } else {
          matrix.put(j, 1);
        }
        _prGraph.put(i, matrix);
      }
      _outLinks.put(j, validOutLinkNum);
    }
    reader.close();
    if (partialFile.exists()) {
      partialFile.delete();
    }
    return;
  }

  private void buildOneDoc(File file, int docId, WriteToFile writer)
      throws IOException {
    HeuristicLinkExtractor linkExtractor = new HeuristicLinkExtractor(file);
    StringBuilder result = new StringBuilder();
    List<String> outGoingLinks = new ArrayList<String>();
    String fileName = linkExtractor.getLinkSource();
    fileName = URIParser.parseFileNameToUTF8(fileName);
    pageIndex.put(fileName, docId);
    String nextLink = null;
    while ((nextLink = linkExtractor.getNextInCorpusLinkTarget()) != null) {
      outGoingLinks.add(nextLink);
    }
    result.append(fileName);
    result.append('\n');
    result.append(outGoingLinks.size());
    result.append('\n');
    for (String outLink : outGoingLinks) {
      result.append(outLink);
      result.append('\n');
    }
    writer.appendToFile(result.toString());
  }

  /**
   * This function computes the PageRank based on the internal graph generated
   * by the {@link prepare} function, and stores the PageRank to be used for
   * ranking.
   * 
   * Note that you will have to store the computed PageRank with each document
   * the same way you do the indexing for HW2. I.e., the PageRank information
   * becomes part of the index and can be used for ranking in serve mode. Thus,
   * you should store the whatever is needed inside the same directory as
   * specified by _indexPrefix inside {@link _options}.
   * 
   * @throws IOException
   */
  @Override
  public void compute() throws IOException {
    System.out.println("Computing using " + this.getClass().getName());
    for (int i = 0; i < pageIndex.keySet().size(); i++) {
      pr.add(1.0f);
    }
    for (int i = 0; i < iteration; i++) {
      for (int j = 0; j < pr.size(); j++) {
        float temp = 0.0f;
        float gammaTemp = 0.0f;
        // compute random page rank value
        for (int k = 0; k < pr.size(); k++) {
          temp += pr.get(k) / pr.size();
        }
        // if other document points to this document
        if (_prGraph.containsKey(j)) {
          // docURI -> outLink _prGraph.get(i) ==> map<j,numLink> j points to i
          for (Map.Entry<Integer, Integer> entry : _prGraph.get(j).entrySet()) {
            int formDocId = entry.getKey();
            float outLinkNum = (float) _outLinks.get(formDocId);
            gammaTemp += entry.getValue() * pr.get(formDocId) / outLinkNum;
          }
        }
        pr.set(j, (1 - gamma) * temp + gamma * gammaTemp);
      }
    }
    writeToFile();
    return;
  }

  // write the page rank value to the file
  public void writeToFile() throws IOException {
    WriteToFile writer = new WriteToFile(PR_FILE);
    StringBuilder result = new StringBuilder();
    Integer size = pageIndex.keySet().size();
    result.append(size);
    result.append('\n');
    writer.appendToFile(result.toString());
    for (Map.Entry<String, Integer> entry : pageIndex.entrySet()) {
      result = new StringBuilder();
      result.append(entry.getKey());
      result.append('\n');
      result.append(pr.get(entry.getValue()));
      result.append('\n');
      writer.appendToFile(result.toString());
    }
    writer.closeBufferWriter();
  }

  /**
   * During indexing mode, this function loads the PageRank values computed
   * during mining mode to be used by the indexer.
   * 
   * @throws IOException
   */
  @Override
  public Map<String, Float> load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    Map<String, Float> prResult = new HashMap<String, Float>();
    if (pr == null || pr.size() == 0) {
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(PR_FILE)));
      int size = Integer.parseInt(br.readLine());
      String content = null;
      while ((content = br.readLine()) != null) {
        float prValue = Float.parseFloat(br.readLine());
        prResult.put(content, prValue);
      }
      br.close();
    } else {
      try {
        throw new IllegalStateException("The PR value is in the class");
      } catch (IllegalStateException ie) {
        ie.printStackTrace();
      }
    }
    return prResult;
  }

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
}
