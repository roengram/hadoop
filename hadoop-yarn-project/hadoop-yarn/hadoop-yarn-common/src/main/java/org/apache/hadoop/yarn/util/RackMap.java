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
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Created by roengram on 15. 5. 21.
 */
public class RackMap<V> extends HashMap<String, V> {


    /*
    Trie structure
     - All leaf trie nodes have values
     - All non-leaf trie nodes do not have values

     */

    private TrieNode root = new TrieNode("ROOT");

    private class TrieNode {
        public String key = null;
        public String value = null;
        public TrieNode parent = null;
        public HashMap<String, TrieNode> children = new HashMap<String, TrieNode>();

        public TrieNode(String key) {
            this.key = key;
        }

        public void print(int indent) {
            for(int i=0 ; i<indent ; i++) System.out.print("  ");
            System.out.println(key + "(" + value + ")");
            Set<String> keys = children.keySet();
            for(String key: keys) {
                TrieNode node = children.get(key);
                node.print(indent+1);
            }
        }
    }

    @Override
    public V put(String key, V value) {
        StringTokenizer keyTok = new StringTokenizer(key, delimiter);
        TrieNode node = root;
        while(keyTok.hasMoreTokens()) {
            String token = keyTok.nextToken();
            if(node.children.containsKey(token) == false) {
                TrieNode newNode = new TrieNode(token);
                newNode.parent = node;
                node.children.put(token, newNode);
            }
            node = node.children.get(token);
        }
        // put value
        if(node.value == null) {
            node.value = new String(key);
        }

        return super.put(key, value);
    }

    public void remove(String key) {
        // remove from trie
        StringTokenizer keyTok = new StringTokenizer(key, delimiter);
        TrieNode node = root;
        while(keyTok.hasMoreTokens()) {
            String token = keyTok.nextToken();
            if(node.children.containsKey(token) == false) {
                assert(super.containsKey(key) == false);
                // no such node exists
                return;
            }
            node = node.children.get(token);
        }
        assert(node != null);

        TrieNode parent = node.parent;
        while(parent != null) {
            parent.children.remove(node.key);
            if(parent.children.isEmpty() == false) {
                break;
            }
            parent = parent.parent;
        }

        // remove from backend map
        super.remove(key);
    }

    public V get(String key) {

        // short cut
        if(super.isEmpty()) {
            return null;
        }
        if(super.containsKey(key)) {
            return super.get(key);
        }

        // we do not have a value that matches the given key exactly
        // return closest one instead

        // get closest ancestor (that has children) of the given key
        StringTokenizer keyTok = new StringTokenizer(key, delimiter);
        TrieNode node = root;
        TrieNode parent = null;
        while(keyTok.hasMoreTokens() && node != null && node.children.isEmpty() == false) {
            String token = keyTok.nextToken();
            parent = node;
            node = node.children.get(token);
        }
        assert(parent.children.size() > 0);

        // return first descendant
        node = parent.children.values().iterator().next();
        while(node.children.isEmpty() == false) {
            node = node.children.values().iterator().next();
        }

        // we should have a leaf node
        assert(node.value != null);
        return super.get(node.value);
    }

    public void print_trie() {
        root.print(0);
    }

    private static String delimiter = "/";

}
