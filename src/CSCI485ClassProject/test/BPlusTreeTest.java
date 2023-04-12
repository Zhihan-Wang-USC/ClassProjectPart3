package CSCI485ClassProject.test;

import CSCI485ClassProject.BPlusTree;
import CSCI485ClassProject.models.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class BPlusTreeTest {

  private static final int NUM_TESTS = 100;
  private static final int MAX_KEY = 10000;
  private static final int MAX_VALUE = 100000;

  @Test
  public void testInsertion() {
    BPlusTree<Integer, Integer> tree = new BPlusTree<Integer,Integer>(2);
    Random random = new Random();
    random.setSeed(123);
    for (int i = 0; i < NUM_TESTS; i++) {
      int key = random.nextInt(MAX_KEY);
      int value = random.nextInt(MAX_VALUE);
      System.out.println("\n\n////Inserting (" + key + ", " + value + ")/////");
      tree.put(key, value);
      tree.visualize();
      Integer result = tree.get(key);
      assertEquals(Integer.valueOf(value),result);
    }
  }

  private static void testDeletion() {
    BPlusTree<Integer, Integer> tree = new BPlusTree<>(4);
    Random random = new Random();
    List<Integer> keys = new ArrayList<>();
    for (int i = 0; i < NUM_TESTS; i++) {
      int key = random.nextInt(MAX_KEY);
      int value = random.nextInt(MAX_VALUE);
      tree.put(key, value);
      keys.add(key);
    }
    for (int key : keys) {
      tree.delete(key);
      assert tree.get(key) == null;
    }
  }

  private static void testBulkInsertion() {
    BPlusTree<Integer, Integer> tree = new BPlusTree<>(4);
    List<Pair<Integer, Integer>> pairs = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < NUM_TESTS; i++) {
      int key = random.nextInt(MAX_KEY);
      int value = random.nextInt(MAX_VALUE);
      pairs.add(new Pair<>(key, value));
    }
    tree.bulkInsert(pairs);
    for (Pair<Integer, Integer> pair : pairs) {
      assert tree.get(pair.getKey()) == pair.getValue();
    }
  }
}
