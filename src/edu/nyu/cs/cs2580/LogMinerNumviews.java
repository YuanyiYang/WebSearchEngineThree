package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class LogMinerNumviews extends LogMiner {
  private final String WORKINGDIR = System.getProperty("user.dir");
  private final String FILE_LOC = WORKINGDIR + "/data/index/numviews";
  private final String NVFILE = WORKINGDIR + "/data/log/20140601-160000.log"; 
  private final String CORPUS_LOC = WORKINGDIR + "/data/wiki";

  private Map<String, Integer> numViews = new HashMap<String, Integer>();

  public LogMinerNumviews(Options options) {
    super(options);
  }

  /**
   * This function processes the logs within the log directory as specified by
   * the {@link _options}. The logs are obtained from Wikipedia dumps and have
   * the following format per line: [language]<space>[article]<space>[#views].
   * Those view information are to be extracted for documents in our corpus and
   * stored somewhere to be used during indexing.
   * 
   * Note that the log contains view information for all articles in Wikipedia
   * and it is necessary to locate the information about articles within our
   * corpus.
   * 
   * @throws IOException
   */
  @Override
  public void compute() throws IOException {
    System.out.println("Computing using " + this.getClass().getName());
    List<File> files = new ArrayList<File>();
    String CORPUS_LOC = _options._corpusPrefix;
    try {
      files = getFilesUnderDirectory(CORPUS_LOC);
    } catch (IOException e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
    for (File file : files) {
      String fileName = URIParser.parseFileNameToUTF8(file.getName());
      if(numViews.containsKey(fileName)){
        try{
          throw new IllegalStateException("This file has been processed!");
        } catch (Exception e){
          System.err.println(e.getMessage());
        }
      }
      numViews.put(fileName,1);
    }
    files = null;
    String NVFILE = _options._logPrefix + "20140601-160000.log";
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(NVFILE)));

    String line = null;
    while ((line = br.readLine()) != null) {
      String[] content = line.split(" ");
      String fileName = URIParser.normalizeURL(content[1]);
      if(numViews.containsKey(fileName)){
        numViews.put(fileName, Integer.parseInt(content[2]));
      }
    }
    br.close();
    ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(FILE_LOC));
    writer.writeObject(numViews);
    writer.close();
    return;
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

  /**
   * During indexing mode, this function loads the NumViews values computed
   * during mining mode to be used by the indexer.
   * 
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Integer> load() throws IOException {
    System.out.println("Loading using " + this.getClass().getName());
    ObjectInputStream reader = new ObjectInputStream(new FileInputStream(FILE_LOC));
    Map<String, Integer> result = null;
    try {
      result = (Map<String, Integer>) reader.readObject();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    reader.close();
    return result;
  }
}
