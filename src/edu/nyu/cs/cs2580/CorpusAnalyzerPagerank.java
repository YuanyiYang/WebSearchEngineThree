package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {
  public CorpusAnalyzerPagerank(Options options) {
    super(options);
  }

  /**
   * This function processes the corpus as specified inside {@link _options}
   * and extracts the "internal" graph structure from the pages inside the
   * corpus. Internal means we only store links between two pages that are both
   * inside the corpus.
   * 
   * Note that you will not be implementing a real crawler. Instead, the corpus
   * you are processing can be simply read from the disk. All you need to do is
   * reading the files one by one, parsing them, extracting the links for them,
   * and computing the graph composed of all and only links that connect two
   * pages that are both in the corpus.
   * 
   * Note that you will need to design the data structure for storing the
   * resulting graph, which will be used by the {@link compute} function. Since
   * the graph may be large, it may be necessary to store partial graphs to
   * disk before producing the final graph.
   *
   * @throws IOException
   */
  
  static public Map<String, Integer> pageIndex = 
                  new LinkedHashMap<String, Integer>();
  static public List<List<Integer>> prGraph = 
                  new ArrayList<List<Integer>>(); //the inlink
  static public List<Integer> outlink = new ArrayList<Integer>();  //the outlink num
  static public List<Float> pr = new ArrayList<Float>();
  
  private static final String WORKINGDIR = System.getProperty("user.dir");
  private static final String prFile = "";   //page rank value file 
  private static final String fileloc = "";  //the location of the wiki text
  private float gamma;  //0.1   0.9
  private int iteration; //1     2
  
  //set the text file location, iteration times, gamma value
  public void setRunEnv(int iteration, float gamma) {
    this.gamma = gamma;
    this.iteration = iteration;
  }
  
  @Override
  public void prepare() throws IOException {
    System.out.println("Preparing " + this.getClass().getName());
    return;
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
        for (Integer docurl: prGraph.get(j)) { //the inlinks of this page
          temp += (pr.get(docurl) / (float)outlink.get(docurl));
        }
        pr.set(j, (1 - gamma) + gamma * temp);
      }
    }
    
    writeToFile();
    return;
  }

  //write the page rank value to the file
  public void writeToFile() throws IOException {
    OutputStreamWriter output = 
      new OutputStreamWriter(new FileOutputStream(prFile, false));
    Integer size = pageIndex.keySet().size();
    output.write(size.toString() + "\n");
    
    //write the document url as the order in the index
    //the whole line as the key
    for (String key: pageIndex.keySet()) {
      output.write(key + "\n");
    }
    
    //write the built graph into the file
    for (List<Integer> list: prGraph) {
      for (Integer i: list) {
        output.write(i.toString() + " ");
      }
      output.write("\n");
    }
    
    //write the number of outlinks 
    for (Integer i: outlink) {
      output.write(i.toString() + " ");
    }
    output.write("\n");
    
    //write the page rank value for each document
    for (Float prValue: pr) {
      output.write(prValue.toString() + " ");
    }
    
    output.close();
  }

  
  /**
   * During indexing mode, this function loads the PageRank values computed
   * during mining mode to be used by the indexer.
   *
   * @throws IOException
   */
  @Override
  public List<Float> load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    
    if (pr.size() != 0) {  //the PageRank value has been loaded
      return pr;
    } else {   //if the PageRank value has not been loaded
      BufferedReader br = 
          new BufferedReader(new InputStreamReader(new FileInputStream(prFile)));
        
      int size = Integer.parseInt(br.readLine());
      int temp = 1;
        
      //load the docurl <-> id map
      for (int i = 0; i < size; i++) {
        String key = br.readLine();
        pageIndex.put(key, temp);
        temp ++;
      }
        
      //load the graph 
      for (int i = 0; i < size; i++) {
        List<Integer> list = new ArrayList<Integer>();
        String line = br.readLine();
        Scanner s = new Scanner(line);
        
        while (s.hasNextInt()) {
          list.add(s.nextInt());
        }
        
        prGraph.add(list);
        s.close();
      }
      
      //load the outlinks
      String line = br.readLine();
      Scanner s = new Scanner(line);
      while (s.hasNextInt()) {
        outlink.add(s.nextInt());
      }
      s.close();
      
      //load the page rank value for each docurl
      line = br.readLine();
      s = new Scanner(line);
      while (s.hasNextFloat()) {
        pr.add(s.nextFloat());
      }
      
      br.close();
      return pr;      
    }
  }
}
