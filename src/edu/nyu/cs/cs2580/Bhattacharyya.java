package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 *  Compute the Bhattacharyya coefficienct between all pairs of queries
 */

public class Bhattacharyya {
  static private final String WORKDIR = System.getProperty("user.dir");
  static private String qrFile = WORKDIR + '/'; //query expansion results
  static private String outputFile = WORKDIR + '/';
  
  private static Map<String, Map<String, Float>> QEmap 
            = new HashMap<String, Map<String, Float>>();
  
  private static void buildQEMap() throws IOException {
    File root = new File(qrFile);
   
    //get all the query expansion results file 
    if (!root.isDirectory()) {
      throw new IOException("The corpus path " + qrFile
                             + " is not a directory!");
    } else {
      File[] subfiles = root.listFiles();
      for (File f : subfiles) {
          String query = f.getName();  
          BufferedReader br = new BufferedReader(new FileReader(f));
          String line;
          Map<String, Float> terms = new HashMap<String, Float>();
          
          while ((line = br.readLine()) != null) {
            Scanner s = new Scanner(line);
            String term = s.next();
            Float value = Float.parseFloat(s.next());
            terms.put(term, value);
            s.close();
          }
          br.close();
          
          QEmap.put(query, terms);
      }
    }
  }
  
  //compute similarity between all pairs of queries
  public static void compute() throws IOException {
    OutputStreamWriter writer = 
        new OutputStreamWriter(new FileOutputStream(outputFile, false));
    String[] terms = (String[]) QEmap.keySet().toArray();
    for (int i = 0; i < terms.length - 1; i++) {
      for (int j = i + 1; j < terms.length; j++) {
        writer.write(terms[i] + '\t' + terms[j] + '\t');
        Float temp = computeSimilarity(QEmap.get(terms[i]), QEmap.get(terms[j]));
        writer.write(temp.toString() + '\n');
      }
    }
    writer.close();
  }
  
  //Compute the similarity between two queries
  public static float computeSimilarity(Map<String, Float> terms1, 
                                 Map<String, Float> terms2) {
    float result = 0.0f;
    
    for (String term: terms1.keySet()) {
      if (terms2.containsKey(term)) {
        result += Math.sqrt(terms1.get(term) * terms2.get(term));
      }
    }
    
    return result;
  }
  
  public static void main(String[] args) throws IOException {
    qrFile += args[1];
    outputFile += args[2];
    buildQEMap();
    compute();
  }

}
