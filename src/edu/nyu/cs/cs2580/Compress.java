package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Compress {

  // Decode occurrence inverted list
  public static Map<Integer, Map<Integer, List<Integer>>> DecodeOccurrenceList(
      byte[] input) {
    Map<Integer, Map<Integer, List<Integer>>> map = new HashMap<Integer, Map<Integer, List<Integer>>>();
    int offset[] = new int[1];
    offset[0] = 0;
    int map_num = DecodeNum(input, offset);

    for (int i = 0; i < map_num; i++) {
      int key = DecodeNum(input, offset);
      Map<Integer, List<Integer>> Occurrence_map = DecodeMap(input, offset);
      map.put(key, Occurrence_map);
    }

    return map;
  }

  // Decode Map<Integer, List<Integer>>
  public static Map<Integer, List<Integer>> DecodeMap(byte[] input, int[] offset) {
    Map<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
    int key_num = DecodeNum(input, offset); // the key of the map
    for (int i = 0; i < key_num; i++) {
      int key = DecodeNum(input, offset); // get the key
      List<Integer> list = DecodeList(input, offset);
      map.put(key, list);
    }

    return map;
  }

  // Docode List<Integer>
  public static List<Integer> DecodeList(byte[] input, int[] offset) {
    List<Integer> list = new ArrayList<Integer>();
    int size = DecodeNum(input, offset);
    for (int i = 0; i < size; i++) {
      int value = DecodeNum(input, offset);
      list.add(value);
    }
    return list;
  }

  // Decode a number
  public static int DecodeNum(byte[] input, int[] offset) {
    int start = offset[0];
    int result = 0;
    while (input[start] >= 0) {
      result += input[start] & 0x7F;
      result <<= 7;
      start++;
    }
    result += input[start] & 0x7F;
    offset[0] = start + 1;
    return result;
  }

  // Encode the occurrence list
  public static ArrayList<Byte> EncodeOccurrenceList(
      Map<Integer, Map<Integer, List<Integer>>> list) {
    ArrayList<Byte> al = new ArrayList<Byte>();
    al.addAll(EncodeNum(list.keySet().size())); // add the total number of keys

    for (Integer key : list.keySet()) {
      al.addAll(EncodeNum(key));
      al.addAll(EncodeList(list.get(key)));
    }

    return al;
  }

  // Encode List<Integer>
  public static ArrayList<Byte> EncodeList(Map<Integer, List<Integer>> list) {
    ArrayList<Byte> al = new ArrayList<Byte>();
    al.addAll(EncodeNum(list.keySet().size())); // add the total number of keys
    for (Integer key : list.keySet()) {
      al.addAll(EncodeNum(key));
      al.addAll(EncodeNum(list.get(key).size()));
      for (Integer v : list.get(key)) {
        al.addAll(EncodeNum(v));
      }
    }
    return al;
  }

  // Encode a number using V-Byte encoding
  public static ArrayList<Byte> EncodeNum(int num) {
    ArrayList<Byte> al = new ArrayList<Byte>();
    int byte_digit = 1; // the number of bytes after compression
    while (num >= (1 << byte_digit * 7)) {
      byte_digit++;
    }

    while (byte_digit > 1) {
      al.add((byte) ((num >> (byte_digit - 1) * 7) & 0x7F));
      byte_digit--;
    }

    al.add((byte) (num | 0x80));

    return al;
  }

  // Encode the given
  public static byte[] EncodeOneTermMap(int key,
      Map<Integer, List<Integer>> map_list) {
    ArrayList<Byte> al = new ArrayList<Byte>();
    if (map_list == null || map_list.size() == 0) {
      al.addAll(EncodeNum(key));
      al.addAll(EncodeNum(0));
    } else {
      al.addAll(EncodeNum(key));
      al.addAll(EncodeNum(map_list.keySet().size()));
      for (Integer index : map_list.keySet()) {
        al.addAll(EncodeNum(index));
        al.addAll(EncodeNum(map_list.get(index).size()));
        for (int i = 0; i < map_list.get(index).size(); i++) {
          al.addAll(EncodeNum(map_list.get(index).get(i)));
        }
      }
    }
    byte[] encoding = new byte[al.size()];
    for (int i = 0; i < al.size(); i++) {
      encoding[i] = al.get(i);
    }
    return encoding;
  }

  // Decode one term Map
  public static Map<Integer, Map<Integer, List<Integer>>> DecodeOneTermMap(
      byte[] input, long offsetNumber) {
    Map<Integer, Map<Integer, List<Integer>>> map = new HashMap<Integer, Map<Integer, List<Integer>>>();

    int offset[] = new int[1];
    offset[0] = (int) offsetNumber;
    int termId = DecodeNum(input, offset);
    Map<Integer, List<Integer>> Occurrence_map = DecodeMap(input, offset);
    map.put(termId, Occurrence_map);
    return map;
  }  

}
