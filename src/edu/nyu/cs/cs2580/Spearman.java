package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Spearman {

  private String pr_loc = null;
  private String num_loc = null;
  private Map<String, Float> pr_rank = new HashMap<String, Float>();
  private Map<String, Integer> num_view = new HashMap<String, Integer>();
  // represent the ranking of the num_view
  private Map<String, Integer> num_ranking = new HashMap<String, Integer>();

  public float similarity = 0.0f;

  public Spearman(String pr_loc, String num_loc) {
    this.pr_loc = pr_loc;
    this.num_loc = num_loc;
    try {
      loadPR();
      loadNum();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    pr_rank = MapUtil.sortedByValue(pr_rank,
        new Comparator<Map.Entry<String, Float>>() {
          @Override
          public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
            if (o1.getValue() < o2.getValue()) {
              return 1;
            } else if (o1.getValue() == o2.getValue()) {
              return o1.getKey().compareTo(o2.getKey());
            } else {
              return -1;
            }
          }
        });
    num_view = MapUtil.sortedByValue(num_view,
        new Comparator<Map.Entry<String, Integer>>() {
          @Override
          public int compare(Entry<String, Integer> o1,
              Entry<String, Integer> o2) {
            if (o1.getValue() > o2.getValue()) {
              return -1;
            } else if (o1.getValue() == o2.getValue()) {
              return o1.getKey().compareTo(o2.getKey());
            } else {
              return 1;
            }
          }
        });
    int ranking = 1;
    for (String key : num_view.keySet()) {
      num_ranking.put(key, ranking);
      ranking++;
    }
  }

  private void loadPR() throws IOException {
    if (pr_loc == null) {
      throw new IllegalArgumentException(
          "The name of pr_file has not been initiated");
    }
    System.out.println("Loading using " + this.getClass().getName());
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(pr_loc)));
    @SuppressWarnings("unused")
    int size = Integer.parseInt(br.readLine());
    String content = null;
    while ((content = br.readLine()) != null) {
      float prValue = Float.parseFloat(br.readLine());
      pr_rank.put(content, prValue);
    }
    br.close();
    if (pr_rank.size() == 0) {
      throw new IllegalStateException("The index load page rank value error");
    }
  }

  @SuppressWarnings("unchecked")
  private void loadNum() throws IOException {
    if (num_loc == null) {
      throw new IllegalArgumentException(
          "The name of num_file has not been initiated");
    }
    System.out.println("Loading using " + this.getClass().getName());
    ObjectInputStream reader = new ObjectInputStream(new FileInputStream(
        num_loc));
    try {
      num_view = (Map<String, Integer>) reader.readObject();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    reader.close();
    if (num_view.size() == 0) {
      throw new IllegalStateException("The index load page rank value error");
    }
  }

  // compute the similarity
  public void computeSim() {
    float z = ((float) (pr_rank.size() + 1)) / 2;
    float up = 0.0f, xdown = 0.0f, ydown = 0.0f;
    float ranking = 1;
    for (String key : pr_rank.keySet()) {
      float x = ranking;
      float y = (float) num_ranking.get(key);
      up += (x - z) * (y - z);
      xdown += (x - z) * (x - z);
      ydown += (y - z) * (y - z);
      ranking++;
    }

    similarity = up / (float) Math.sqrt(xdown * ydown);
    System.out.format("%.5f", similarity);
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "You should have two arguments for this class");
    }
    String prFileLoc = args[0];
    String numFileLoc = args[1];
    Spearman spearman = new Spearman(prFileLoc, numFileLoc);
    spearman.computeSim();
  }

}
