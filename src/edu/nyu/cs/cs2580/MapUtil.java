package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtil {

  /**
   * 
   * @param map
   * @return linkedHashMap to ensure the iteration sequence
   */
  public static <K, V extends Comparable<? super V>> Map<K, V> sortedByValue(
      Map<K, V> map) {
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      @Override
      public int compare(Entry<K, V> o1, Entry<K, V> o2) {
        return o2.getValue().compareTo(o1.getValue());
      }
    });
    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  /**
   * 
   * @param map
   * @param comparator
   *          the comparator used to sort value
   * @return
   */
  public static <K, V> Map<K, V> sortedByValue(Map<K, V> map,
      Comparator<? super Map.Entry<K, V>> comparator) {
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, comparator);
    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  public static void main(String[] args) {
    Map<String, Integer> map = new HashMap<String, Integer>();
    for (int i = 1; i <= 200; i++) {
      map.put(i + "", i);
    }
    Map<String, Integer> result = MapUtil.sortedByValue(map);
    for (Map.Entry<String, Integer> entry : result.entrySet()) {
      System.out.println(entry.getKey() + " + " + entry.getValue());
    }
    int i = 0;
    Iterator<Map.Entry<String, Integer>> iterator = result.entrySet()
        .iterator();
    while (i < 10 && iterator.hasNext()) {
      Map.Entry<String, Integer> entry = iterator.next();
      System.out.println(entry.getKey() + " + " + entry.getValue());
      i++;
    }
  }
}
