/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * A Map whose key is rack id - a sequence of strings delimited by '/'.
 * This Map performs longest-prefix matching against a given key to find a value.
 * This ensures nodes that are closest (in terms of network distance)
 * to the given rack id are returned.
 */
public class RackMap<V> {

  /**
   * Root of internal Trie structure
   */
  private TrieNode root = new TrieNode("");

  /**
   * Delimiter of rack id's components
   */
  private static String delimiter = "/";

  /**
   * Backend HashMap
   */
  private HashMap<String, V> rawMap = new HashMap<String, V>();

  /**
   * Node for Trie structure.
   * {@link org.apache.hadoop.yarn.util.RackMap.TrieNode#key}
   * corresponds to a single component of rack id,
   * and {@link org.apache.hadoop.yarn.util.RackMap.TrieNode#value}
   * is a rack id composed of keys of all ancestors and this node.
   * Only a leaf TrieNode has value.
   */
  private class TrieNode {
    public String key = null;
    public String value = null;
    public TrieNode parent = null;
    public HashMap<String, TrieNode> children = new HashMap<String, TrieNode>();

    public TrieNode(String key) {
      this.key = key;
    }

    public void clearRecursive() {
      key = null;
      value = null;
      parent = null;

      Set<String> keys = children.keySet();
      Iterator<String> it = keys.iterator();
      while (it.hasNext()) {
	TrieNode n = children.get(it.next());
	n.clearRecursive();
      }
      children.clear();
    }
  }

  /**
   * Put a key and value to this Map.
   * @param key Rack ID
   * @param value value
   * @return previous value associated with the given key
   */
  public V put(String key, V value) {
    StringTokenizer keyTok = new StringTokenizer(key, delimiter);
    TrieNode node = root;
    while (keyTok.hasMoreTokens()) {
      String token = keyTok.nextToken();
      if (node.children.containsKey(token) == false) {
	// insert new TrieNode
	TrieNode newNode = new TrieNode(token);
	newNode.parent = node;
	node.children.put(token, newNode);
      }
      node = node.children.get(token);
    }
    // set leaf TrieNode's value to the given key
    if (node.value == null) {
      node.value = new String(key);
    }

    return rawMap.put(key, value);
  }

  /**
   * Remove key-value pair associated with the given key
   * @param key
   * @return previous value associated with key
   */
  public V remove(String key) {
    // remove from trie
    StringTokenizer keyTok = new StringTokenizer(key, delimiter);
    TrieNode node = root;
    while (keyTok.hasMoreTokens()) {
      String token = keyTok.nextToken();
      if (node.children.containsKey(token) == false) {
	assert (rawMap.containsKey(key) == false);
	// no such node exists
	return null;
      }
      node = node.children.get(token);
    }
    if (node == null) {
      // no such node exists
      return null;
    }

    TrieNode parent = node.parent;
    while (parent != null) {
      parent.children.remove(node.key);
      if (parent.children.isEmpty() == false) {
	break;
      }
      parent = parent.parent;
    }

    // remove from backend map
    return rawMap.remove(key);
  }

  /**
   * Return a value associated with key.
   * Performs longest prefix matching based on Trie structure.
   * @param key rack id to find a value for.
   * @return value that is associated with a key, which is closest (NW distance) to the given key
   */
  public V get(String key) {

    // short cut
    if (rawMap.isEmpty()) {
      return null;
    }
    if (rawMap.containsKey(key)) {
      return rawMap.get(key);
    }

    // We do not have a value that exactly matches the given key
    // Search for the closest one instead

    // Get closest ancestor (with children) of the given key
    StringTokenizer keyTok = new StringTokenizer(key, delimiter);
    TrieNode node = root;
    TrieNode parent = null;
    while (keyTok.hasMoreTokens() && node != null
		    && node.children.isEmpty() == false) {
      String token = keyTok.nextToken();
      parent = node;
      node = node.children.get(token);
    }
    assert (parent.children.size() > 0);

    // return first descendant
    node = parent.children.values().iterator().next();
    while (node.children.isEmpty() == false) {
      node = node.children.values().iterator().next();
    }

    // we should have a leaf node
    assert (node.value != null);
    return rawMap.get(node.value);
  }

  /**
   * Clear this Map
   */
  public void clear() {
    root.clearRecursive();
    rawMap.clear();
  }

  /**
   * Return the number of key-value pairs stored in this Map
   */
  public int size() {
    return rawMap.size();
  }
}
