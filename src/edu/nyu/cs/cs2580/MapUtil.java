package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MapUtil {

  /*
   * PECS stands for producer-extends, consumer-super. If a parameterized type
   * represents a T producer, use <? extends T>; if it represents a T consumer,
   * use <? super T>. In my own words: let's say in the function, we use the
   * element from the parameters as type T. It's fine that it's actually a
   * subtype of T since any where we expect T, we could use it's subtype. That
   * means producer extends. When we return a T type, the consumer receives the
   * type of T. Actually it could be supertype of T since anywhere we expect a
   * supertype of T, we could use type T.
   * 
   * In this function, a wildcard applied to a type parameter. V was originally
   * specified to extend Comparable<V>, but a comparable of V consumes V
   * instances and produces integers indicating order relations. Comparables are
   * always consumers, so we could always use Comparable<? super V> in
   * preference to Comparable<V>. The same is true of comparators, we should
   * always use Comparator<? super V> in preference to Comparator<V>
   */
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
        return o1.getValue().compareTo(o2.getValue());
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
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K,V>>(map.entrySet());
    Collections.sort(list, comparator);
    Map<K,V> result = new LinkedHashMap<K,V>();
    for(Map.Entry<K, V> entry : list){
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
