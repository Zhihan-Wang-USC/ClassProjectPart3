package CSCI485ClassProject;

import CSCI485ClassProject.models.Pair;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;


public class BPlusTree<K extends Comparable<K>, V> {

  private interface Node<K extends Comparable<K>, V> {
    V get(K key);

    Node<K, V> put(K key, V value);

    Node<K, V> delete(K key);
  }

  private class InternalNode<K extends Comparable<K>, V> implements Node<K, V> {
    private List<K> keys;
    private List<Node<K, V>> children;
//    private Integer level;

    public InternalNode(List<K> keys, List<Node<K, V>> children) {
      this.keys = keys;
      this.children = children;
    }

    public V get(K key) {
      int index = Collections.binarySearch(keys, key);
      if (index < 0) {
        index = -index - 1;
      }
      else{
        index++;
      }
      return children.get(index).get(key);
    }

    public Node<K, V> put(K key, V value) {
      int index = Collections.binarySearch(keys, key);
      if (index < 0) {
        index = -index - 1;
      }
      Node<K, V> child = children.get(index);
      Node<K, V> newChild = child.put(key, value);
      if (child == newChild){
        return this;
      }

      List<K> newKeys = new ArrayList<K>(keys);
      List<Node<K, V>> newChildren = new ArrayList<Node<K, V>>(children);

      if (child instanceof LeafNode && newChild instanceof InternalNode) {
        newChildren.set(index, newChild);
//        if (index > 0) {
//          newKeys.set(index - 1, ((InternalNode<K, V>) newChild).keys.get(0));
//        }
        return new InternalNode<K, V>(newKeys, newChildren);
      }
      if (child instanceof InternalNode){
        InternalNode newLeft = (InternalNode) ((InternalNode<K,V>) newChild).children.get(0);
        InternalNode newRight = (InternalNode) ((InternalNode<K,V>) newChild).children.get(1);
        newChildren.set(index, newLeft);
        newChildren.add(index, newRight);
        return new InternalNode<K, V>(newKeys, newChildren);
      }

//      if (child instanceof InternalNode && newChild instanceof InternalNode) {
//        newChildren.set(index, newChild);
//        if (index < keys.size()) {
//          newKeys.set(index, ((LeafNode<K, V>) newChild).keys.get(0));
//        }
//      } else if (child instanceof InternalNode){
//        newChildren.set(index, newChild);
//      } else {
//        newKeys.add(index, ((LeafNode<K, V>) newChild).keys.get(0));
//      }

//
//      if (newChild != child) {
//        List<K> newKeys = new ArrayList<K>(keys);
//        List<Node<K, V>> newChildren = new ArrayList<Node<K, V>>(children);
//
//
//        else if (child instanceof InternalNode){
//          newChildren.set(index, newChild);
//        }
//        else {
//          newKeys.add(index, ((LeafNode<K, V>) newChild).keys.get(0));
//        }
//        if (newKeys.size() > order) {
//          int mid = newKeys.size() / 2;
//          List<K> rightKeys = new ArrayList<>( newKeys.subList(mid + 1, newKeys.size()));
//          List<Node<K, V>> rightChildren = new ArrayList<>( newChildren.subList(mid + 1, newChildren.size()));
//          newKeys = new ArrayList<>( newKeys.subList(0, mid));
//          newChildren = new ArrayList<> (newChildren.subList(0, mid + 1));
//          return new InternalNode<K, V>(newKeys, newChildren);
//        } else {
//          return new InternalNode<K, V>(newKeys, newChildren);
//        }
//      } else {
//        return this;
//      }
      return this;
    }

    public Node<K, V> delete(K key) {
      int index = Collections.binarySearch(keys, key);
      if (index < 0) {
        index = -index - 1;
      }
      Node<K, V> child = children.get(index);
      Node<K, V> newChild = child.delete(key);
      if (newChild != child) {
        List<K> newKeys = new ArrayList<K>(keys);
        List<Node<K, V>> newChildren = new ArrayList<Node<K, V>>(children);
        newChildren.set(index, newChild);
        if (newChild instanceof InternalNode) {
          newKeys.set(index, ((InternalNode<K, V>) newChild).keys.get(0));
        } else {
          newKeys.set(index, ((LeafNode<K, V>) newChild).keys.get(0));
        }
        if (newKeys.size() < (order + 1) / 2) {
          if (index == 0) {
            InternalNode<K, V> sibling = (InternalNode<K, V>) children.get(1);
            if (sibling.keys.size() > (order + 1) / 2) {
              List<K> siblingKeys = sibling.keys;
              List<Node<K, V>> siblingChildren = sibling.children;
              newKeys.add(siblingKeys.get(0));
              newChildren.add(siblingChildren.get(0));
              siblingKeys.remove(0);
              siblingChildren.remove(0);
              sibling.keys = siblingKeys;
              sibling.children = siblingChildren;
              sibling.updateParentKeys();
              return this;
            } else {
              List<K> mergedKeys = new ArrayList<K>(newKeys);
              List<Node<K, V>> mergedChildren = new ArrayList<Node<K, V>>(newChildren);
              mergedKeys.add(sibling.keys.get(0));
              mergedKeys.addAll(sibling.keys.subList(1, sibling.keys.size()));
              mergedChildren.addAll(sibling.children);
              sibling.keys.clear();
              sibling.children.clear();
              InternalNode<K, V> merged = new InternalNode<K, V>(mergedKeys, mergedChildren);
              merged.updateParentKeys();
              return merged;
            }
          } else {
            InternalNode<K, V> leftSibling = (InternalNode<K, V>) children.get(index - 1);
            if (leftSibling.keys.size() > (order + 1) / 2) {
              List<K> leftSiblingKeys = leftSibling.keys;
              List<Node<K, V>> leftSiblingChildren = leftSibling.children;
              newKeys.add(0, leftSiblingKeys.get(leftSiblingKeys.size() - 1));
              newChildren.add(0, leftSiblingChildren.get(leftSiblingChildren.size() - 1));
              leftSiblingKeys.remove(leftSiblingKeys.size() - 1);
              leftSiblingChildren.remove(leftSiblingChildren.size() - 1);
              leftSibling.keys = leftSiblingKeys;
              leftSibling.children = leftSiblingChildren;
              leftSibling.updateParentKeys();
              return this;
            } else {
              List<K> mergedKeys = new ArrayList<K>(leftSibling.keys);
              List<Node<K, V>> mergedChildren = new ArrayList<Node<K, V>>(leftSibling.children);
              mergedKeys.add(keys.get(index - 1));
              mergedKeys.addAll(newKeys);
              mergedChildren.addAll(newChildren);
              keys.remove(index - 1);
              children.remove(index);
              leftSibling.keys = mergedKeys;
              leftSibling.children = mergedChildren;
              leftSibling.updateParentKeys();
              return leftSibling;
            }
          }
        } else {
          return this;
        }
      } else {
        return this;
      }
    }

    private void updateParentKeys() {
      for (int i = 0; i < children.size(); i++) {
        Node<K, V> child = children.get(i);
        if (child instanceof InternalNode) {
          keys.set(i, ((InternalNode<K, V>) child).keys.get(0));
        } else {
          keys.set(i, ((LeafNode<K, V>) child).keys.get(0));
        }
      }
    }
  }

  private class LeafNode<K extends Comparable<K>, V> implements Node<K, V> {
    private List<K> keys;
    private List<V> values;
    private LeafNode<K, V> next;

    public LeafNode() {
      this.keys = new ArrayList<K>();
      this.values = new ArrayList<V>();
    }

    public V get(K key) {
      int index = Collections.binarySearch(keys, key);
      if (index >= 0) {
        return values.get(index);
      } else {
        return null;
      }
    }

    public Node<K, V> put(K key, V value) {
      int index = binaryFindIndex(key);
      keys.add(index, key);
      values.add(index, value);
      if (keys.size() > order) {
        int mid = keys.size() / 2;
        List<K> rightKeys = new ArrayList<>( keys.subList(mid, keys.size()));
        List<V> rightValues = new ArrayList<>( values.subList(mid, values.size()));
        keys = new ArrayList<>(keys.subList(0, mid));
        values = new ArrayList<>(values.subList(0, mid));
        LeafNode<K, V> right = new LeafNode<K, V>();
        right.keys = rightKeys;
        right.values = rightValues;
        right.next = next;
        next = right;
        return new InternalNode<K, V>(Collections.singletonList(rightKeys.get(0)),
                Arrays.asList(this, right));
      }

      return this;
    }

    private int binaryFindIndex(K key) {
      int index = Collections.binarySearch(keys, key);
      if (index < 0) {
        index = -index - 1;
      }
      return index;
    }

    private int linearFindIndex(K key) {
      int index = 0;
      while (index < keys.size() && key.compareTo(keys.get(index)) > 0) {
        index++;
      }
      return index;
    }

    public Node<K, V> delete(K key) {
      int index = Collections.binarySearch(keys, key);
      if (index >= 0) {
        keys.remove(index);
        values.remove(index);
        if (keys.size() < (order + 1) / 2) {
          if (next != null && next.keys.size() > (order + 1) / 2) {
            keys.add(next.keys.get(0));
            values.add(next.values.get(0));
            next.keys.remove(0);
            next.values.remove(0);
            return this;
          } else {
            if (next != null) {
              keys.addAll(next.keys);
              values.addAll(next.values);
              next = next.next;
            }
            return this;
          }
        } else {
          return this;
        }
      } else {
        return this;
      }
    }
  }


  private int order;
  private Node<K, V> root;

  public BPlusTree(int order) {
    this.order = order;
    this.root = new LeafNode<K, V>();
  }

  public V get(K key) {
    return root.get(key);
  }

  public void put(K key, V value) {
    root = root.put(key, value);
  }

  public void bulkInsert(List<Pair<K, V>> pairs) {
    for (Pair<K, V> pair : pairs) {
      put(pair.getKey(), pair.getValue());
    }
  }

  public void delete(K key) {
    root = root.delete(key);
  }

  public void visualize() {
    Node<K, V> rootCopy = this.root; // Make a copy of the root
    if (rootCopy instanceof InternalNode) {
      visualizeInternalNode((InternalNode<K, V>) rootCopy, "");
    } else {
      visualizeLeafNode((LeafNode<K, V>) rootCopy, "");
    }
  }

  private void visualizeInternalNode(InternalNode<K, V> node, String indent) {
    System.out.println(indent + "InternalNode");
    indent += "  ";
    for (int i = 0; i < node.children.size(); i++) {
      Node<K, V> child = node.children.get(i);
      if (child instanceof InternalNode) {
        InternalNode<K, V> internalChild = (InternalNode<K, V>) child;
        visualizeInternalNode(internalChild, indent + "  ");
      } else {
        LeafNode<K, V> leafChild = (LeafNode<K, V>) child;
        visualizeLeafNode(leafChild, indent + "  ");
      }
      if (i < node.keys.size()) {
        System.out.println(indent + "Key: " + node.keys.get(i));
      }
    }
  }

  private void visualizeLeafNode(LeafNode<K, V> node, String indent) {
    System.out.println(indent + "LeafNode");
    indent += "  ";
    Iterator<K> keyIterator = node.keys.iterator();
    Iterator<V> valueIterator = node.values.iterator();
    while (keyIterator.hasNext()) {
      System.out.println(indent + "Key: " + keyIterator.next() + ", Value: " + valueIterator.next());
    }
  }
}



