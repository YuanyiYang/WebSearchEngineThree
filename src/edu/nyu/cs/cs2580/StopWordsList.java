package edu.nyu.cs.cs2580;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Scanner;

public class StopWordsList {

  static HashSet<String> stopwordsSet = null;

  static boolean isStopWord(String s) {
    if (stopwordsSet == null) {
      stopwordsSet = new HashSet<String>();
      FileReader fin = null;
      try {
        fin = new FileReader("common-english-words.txt");
      } catch (FileNotFoundException e) {
        System.err.println(e.getMessage());
      }
      Scanner scanner = new Scanner(fin);
      scanner.useDelimiter(",");
      while (scanner.hasNext()) {
        String temp = scanner.next();
        stopwordsSet.add(temp);
      }
      scanner.close();
    }
    if (stopwordsSet.contains(s))
      return true;
    else
      return false;
  }
}
