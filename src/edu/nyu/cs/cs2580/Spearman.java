package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/****************
 * 
 * Compute the Spearman's rank correlation coefficient
 * of PageRank and NumViews feature to get the similarity
 * 
 * @author xzysolitaire
 *
 */

public class Spearman {
  private List<kvpair> pr = new ArrayList<kvpair>();
  private List<kvpair> nv = new ArrayList<kvpair>();
  private Map<Integer, Float> pr_rank = new HashMap<Integer, Float>();
  private Map<Integer, Float> nv_rank = new HashMap<Integer, Float>();
  
  static public float similarity = 0.0f;
  
  class kvpair implements Comparable<kvpair> {
    public int key;
    public float value;
    
    public kvpair(Integer key, Float value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public int compareTo(kvpair kvp) {
      if (this.value < kvp.value) {
        return 1;
      } else if (this.value == kvp.value) {
        return 0;
      } else {
        return -1;
      }
    }
  }
  
  //compute the similarity
  private void computeSim() {
    float z = (pr.size() + 1) / 2;
    float up = 0.0f, xdown = 0.0f, ydown = 0.0f;
    
    for (int i = 1; i <= pr.size(); i++) {
      float x = pr_rank.get(i);
      float y = nv_rank.get(i);
      up += (x - z) * (y - z);
      xdown += (x - z) * (x - z);
      ydown += (y - z) * (y - z);
    }
    
    similarity = up / (xdown * ydown);
  }
  
  //input is the list for page rank value and numviews
  public Spearman(List<Float> pr_v, List<Integer> nv_v) {
    for (int i = 0; i < pr_v.size(); i++) {
      pr.add(new kvpair(i + 1, pr_v.get(i)));
    }
    for (int i = 0; i < nv_v.size(); i++) {
      nv.add(new kvpair(i + 1, (float)nv_v.get(i)));
    }
    Collections.sort(pr);
    Collections.sort(nv);
    
    for (int i = 0; i < pr.size(); i++) {
      pr_rank.put(pr.get(i).key, (float)i + 1);
    }
    for (int i = 0; i < nv.size(); i++) {
      nv_rank.put(nv.get(i).key, (float)i + 1);
    }
    
    computeSim();
  }
  
  public static void main(String[] args) {  

  }

}
