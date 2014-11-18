package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URIParser {

  public static String charset = (String) System.getProperties().get(
      "file.encoding");

  public static String parseFileNameToUTF8(String str) {
    String result = null;
    try {
      result = new String(str.getBytes(charset), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return result;
  }

  public static String normalizeURL(String str) {
    String temp = null;
    try {
      temp = java.net.URLDecoder.decode(str, "UTF-8");
    } catch (Exception e) {

    }
    int beginIndex = 0;
    int endIndex = temp.length();
    int hashIndex = temp.indexOf('#');
    if (hashIndex > 0) {
      temp = temp.substring(beginIndex, hashIndex);
      endIndex = hashIndex - 1;
    }
    int colonIndex = temp.indexOf(':');
    if (colonIndex > 0) {
      beginIndex = colonIndex;
      if (temp.startsWith("//", beginIndex)) {
        beginIndex = colonIndex + 2;
      }
      temp = temp.substring(beginIndex);
    }
    int questionIndex = temp.indexOf('?');
    if (questionIndex > 0) {
      temp = temp.substring(beginIndex, questionIndex);
      endIndex = questionIndex - 1;
    }
    int semicolonIndex = temp.indexOf(';');
    if (semicolonIndex > 0) {
      temp = temp.substring(beginIndex, semicolonIndex);
      endIndex = semicolonIndex - 1;
    }
    int pathIndex = temp.indexOf('/');
    if (pathIndex == -1) {
      return temp;
    } else {
      Pattern p = Pattern.compile("[^ /]*/([^ /]*)$");
      Matcher matcher = p.matcher(temp.substring(pathIndex));
      if (matcher.find()) {
        temp = matcher.group(1);
      }
      return temp;
    }
  }

  public static void main(String[] args) {
    Charset charset = Charset.defaultCharset();
    System.out.println(charset.toString());
    String filePath = "data/log/20140601-160000.log";
    File file = new File(filePath);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    int count = 1;
    while (count < 20000) {
      count++;
      String[] content = null;
      try {
        content = reader.readLine().split(" ");
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      try {
        // System.out.println(normalizeURL(content[1]));
        System.out.println(java.net.URLDecoder.decode(content[1], "UTF-8"));
      } catch (Exception e) {

      }
    }
    try {
      reader.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
