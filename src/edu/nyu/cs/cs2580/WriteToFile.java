package edu.nyu.cs.cs2580;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

class WriteToFile {

  private File file;
  private BufferedWriter bw = null; // the writer now solely append content to
                                    // file

  public WriteToFile(String file_path) {
    try {
      file = new File(file_path);
      if (!file.exists()) {
        file.createNewFile();
      }
      bw = new BufferedWriter(new FileWriter(file, true));
    } catch (IOException ioe) {
      System.err.println("Oops: " + ioe.getMessage());
      ioe.printStackTrace();
    }
  }
  
  /**
   * Should call this method after appending contents to files
   */
  public void closeBufferWriter() {
    if (bw != null) {
      try {
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * It will overrite the original content.
   * 
   * @param content
   */
  public void writeToFile(String content) {
    try {
      FileWriter fw = new FileWriter(file);
      bw = new BufferedWriter(fw);
      bw.write(content);
      bw.flush();
    } catch (IOException ioe) {
      System.err.println("Oops" + ioe.getMessage());
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          System.err.println("Oops: " + e.getMessage());
        }
      }
    }
  }

  /**
   * It will append the content to the end of the file.
   */
  public void appendToFile(String content) {
    try {
      FileWriter fw = new FileWriter(file, true);
      bw = new BufferedWriter(fw);
      bw.write(content);
      bw.flush();
    } catch (IOException ioe) {
      System.err.println("Oops: " + ioe.getMessage());
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          System.err.println("Oops: " + e.getMessage());
        }
      }
    }
  }

  public void appendListToFile(Map<Integer, List<Integer>> invertedMap) {
    if (invertedMap == null || invertedMap.size() == 0)
      return;
    try {
      FileWriter fw = new FileWriter(file, true);
      bw = new BufferedWriter(fw);
      for (Integer i : invertedMap.keySet()) {
        // each line
        StringBuilder sb = new StringBuilder();
        sb.append(i);
        sb.append(' ');
        List<Integer> postingList = invertedMap.get(i);
        sb.append(postingList.size());
        sb.append(' ');
        for (int j = 0; j < postingList.size(); j++) {
          sb.append(postingList.get(j));
          if (j == postingList.size() - 1) {
            sb.append('\n');
          } else {
            sb.append(' ');
          }
        }
        bw.write(sb.toString());
        bw.flush();
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          System.err.println(e.getMessage());
        }
      }
    }
  }

  /**
   * write each term to file system
   * 
   * @param termId
   *          the id of the term
   * @param oneTermMap
   *          the inner map of the inverted map, ie the map that map the key
   *          value is the document id and value is the occurence position
   */
  public void appendOneTermMapToFile(int termId,
      Map<Integer, List<Integer>> oneTermMap) {
    if (oneTermMap == null || oneTermMap.size() == 0)
      return;
    int allDocNums = oneTermMap.keySet().size();
    StringBuilder toFileString = new StringBuilder();
    toFileString.append(termId);
    toFileString.append(' ');
    toFileString.append(allDocNums);
    toFileString.append(' ');
    for (Map.Entry<Integer, List<Integer>> entry : oneTermMap.entrySet()) {
      int docId = entry.getKey();
      List<Integer> occurenceList = entry.getValue();
      if (occurenceList == null || occurenceList.size() == 0)
        continue;
      int occurenceTime = occurenceList.size();
      toFileString.append(docId);
      toFileString.append(' ');
      toFileString.append(occurenceTime);
      toFileString.append(' ');
      for (int k = 0; k < occurenceList.size(); k++) {
        toFileString.append(occurenceList.get(k)); // every postion
        toFileString.append(' ');
      }
    }
    toFileString.append('\n');
    // write the stringBuilder to file
    try {
      bw.write(toFileString.toString());
      bw.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void appendMapToFile(Map<Integer, Map<Integer, List<Integer>>> map) {
    if (map == null || map.size() == 0)
      return;
    try {
      List<Integer> positionList = new ArrayList<Integer>();
      for (Integer i : map.keySet()) { // one line
        // each term has one StringBuffer
        StringBuilder sb = new StringBuilder();
        sb.append(i); // term id
        sb.append(' ');
        sb.append(map.get(i).size()); // num of docs
        sb.append(' ');
        for (Integer j : map.get(i).keySet()) {
          sb.append(j); // doc id
          sb.append(' ');
          positionList = map.get(i).get(j);
          sb.append(positionList.size()); // num of positions
          sb.append(' ');
          for (int k = 0; k < positionList.size(); k++) {
            sb.append(positionList.get(k)); // every postion
            sb.append(' ');
          }
        }
        sb.append('\n');
        // write the stringBuilder to file
        bw.write(sb.toString());
        bw.flush();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      closeBufferWriter();
    }
    //System.out.println("output to file.");
  }

  public static void main(String[] args) {
    WriteToFile w = new WriteToFile("test");
    Map<Integer, Map<Integer, List<Integer>>> map = new HashMap<Integer, Map<Integer, List<Integer>>>();
    Map<Integer, List<Integer>> term1ToDoc = new HashMap<Integer, List<Integer>>();
    ArrayList<Integer> Term1doc1Occur = new ArrayList<Integer>();
    for (int i = 0; i < 20; i++) {
      Term1doc1Occur.add(i);
    }
    ArrayList<Integer> Term1doc2Occur = new ArrayList<Integer>();
    for (int i = 0; i < 20; i++) {
      Term1doc2Occur.add(i * 2);
    }
    ArrayList<Integer> Term1doc3Occur = new ArrayList<Integer>();
    for (int i = 0; i < 20; i++) {
      Term1doc3Occur.add(i * 3);
    }
    ArrayList<Integer> Term1doc4Occur = new ArrayList<Integer>();
    for (int i = 0; i < 20; i++) {
      Term1doc4Occur.add(i * 4);
    }
    term1ToDoc.put(1, Term1doc1Occur);
    term1ToDoc.put(2, Term1doc2Occur);
    term1ToDoc.put(3, Term1doc3Occur);
    term1ToDoc.put(4, Term1doc4Occur);
    map.put(1, term1ToDoc);
    map.put(2, term1ToDoc);
    w.appendMapToFile(map);
  }
}
