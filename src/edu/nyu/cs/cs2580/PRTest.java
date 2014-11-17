package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class PRTest extends CorpusAnalyzer {

  private final String WORKINGDIR = System.getProperty("user.dir");
  private final String PR_File = WORKINGDIR + "/data/index"; // page rank value
                                                             // file
  private final String CORPUS_LOC = WORKINGDIR + "/data/wiki"; // the location
                                                               // of the wiki
                                                               // text
  private final String PARTIAL_PRFILE = WORKINGDIR + "/parts/PartialPRGraph";
  private float gamma; // 0.1 0.9
  private int iteration; // 1 2
  // file name(URI) --> docId
  public Map<String, Integer> pageIndex = new LinkedHashMap<String, Integer>();

  // _prGraph.get(i) ==> map<j,numLink> represents how many link(numLink)
  // document j has that points to document i
  public Map<Integer, Map<Integer, Integer>> _prGraph = new HashMap<Integer, Map<Integer, Integer>>();

  // num = outLink.get(i) represent how many outgoinglink i has
  public Map<Integer, Integer> _outLinks = new HashMap<Integer, Integer>();

  public List<Float> pr = new ArrayList<Float>();

  public PRTest(Options options) {
    super(options);
  }

  public PRTest() {
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

  // set the text file location, iteration times, gamma value
  public void setRunEnv(int iteration, float gamma) {
    this.gamma = gamma;
    this.iteration = iteration;
  }

  @Override
  public void prepare() throws IOException {
    System.out.println("Preparing " + this.getClass().getName());
    List<File> files = new ArrayList<File>();
    try {
      files = getFilesUnderDirectory(CORPUS_LOC);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
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
        outLinks--;
      }
      _outLinks.put(j, validOutLinkNum);
    }
    reader.close();
    /*
     * File partialFile = new File(PARTIAL_PRFILE); if (partialFile.exists()) {
     * partialFile.delete(); }
     */
    return;
  }

  private void buildOneDoc(File file, int docId, WriteToFile writer)
      throws IOException {
    HeuristicLinkExtractor linkExtractor = new HeuristicLinkExtractor(file);
    StringBuilder result = new StringBuilder();
    List<String> outGoingLinks = new ArrayList<String>();
    String fileName = linkExtractor.getLinkSource();
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
        // docURI -> outLink _prGraph.get(i) ==> map<j,numLink> j points to i
        for (Map.Entry<Integer, Integer> entry : _prGraph.get(j).entrySet()) {
          int formDocId = entry.getKey();
          float outLinkNum = (float) _outLinks.get(formDocId);
          temp += entry.getValue() * pr.get(formDocId) / outLinkNum;
        }
        // for (Integer docurl : prGraph.get(j)) { // the inlinks of this page
        // temp += (pr.get(docurl) / (float) outlink.get(docurl));
        // }
        pr.set(j, (1 - gamma) + gamma * temp);
      }
    }
    writeToFile();
    return;
  }

  // write the page rank value to the file
  public void writeToFile() throws IOException {
    WriteToFile writer = new WriteToFile(PR_File);
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
          new FileInputStream(PR_File)));
      int size = Integer.parseInt(br.readLine());
      String content = null;
      while((content=br.readLine())!=null){
        float prValue = Float.parseFloat(br.readLine());
        prResult.put(content, prValue);
      }
    } else {
      try{
        throw new IllegalStateException("The PR value is in the class");
      } catch (IllegalStateException ie){
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

  public static void main(String[] args) {

  }
}
