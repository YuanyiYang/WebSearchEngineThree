package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
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
  
  // query --> query expansino result file
  private static Map<String, String> QEFile = new HashMap<String, String>();
  // query --> term / probability
  private static Map<String, Map<String, Float>> QEmap 
            = new HashMap<String, Map<String, Float>>();
  
  private static void getQEFile() throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(qrFile));
    String line;
    
    while ((line = br.readLine()) != null) {
      String[] temp = line.split(".");
      if (temp.length == 2) {
        QEFile.put(temp[0], temp[1]);        
      }
    }
    
    br.close();
  }
  
  //Build the query --> query expansion map
  private static void buildQEMap() throws IOException {
    for (String query: QEFile.keySet()) {
      String f = WORKDIR + '/' + QEFile.get(query); //file location
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
      
      QEmap.put(query, terms);
      br.close();
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
  	if (args.length != 2) {
  		throw new IllegalArgumentException("You should have two arguments");
  	}
  	qrFile += args[0];
  	outputFile += args[1];
  	getQEFile();
  	buildQEMap();
  	compute();
  }
}
