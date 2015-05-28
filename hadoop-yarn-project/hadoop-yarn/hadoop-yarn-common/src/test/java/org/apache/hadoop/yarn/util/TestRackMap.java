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

import org.junit.Assert;
import org.junit.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TestRackMap {

    private static final Log LOG =
            LogFactory.getLog(TestRackMap.class);

    @Test
    public void testEmpty() {
        RackMap<String> rm = new RackMap<String>();
        Assert.assertEquals(null, rm.get("/non-exist"));
        Assert.assertEquals("Empty RackMap should have 0 element", 0, rm.size());
    }

    @Test
    public void testOneLevelTrie() {
        RackMap<String> rm = new RackMap<String>();

        rm.put("/a", "/a");
        rm.put("/b", "/b");

        Assert.assertEquals("Inserted key/value should exist", "/a", rm.get("/a"));
        Assert.assertEquals("Inserted key/value should exist", "/b", rm.get("/b"));
    }

    @Test
    public void testRemoveOneLevel() {
        RackMap<String> rm = new RackMap<String>();

        rm.put("/a", "/a");
        rm.remove("/a");
        Assert.assertEquals("Removed key/value should not exist", null, rm.get("/a"));
    }

    @Test
    public void testReturnClosestLeafNode() {
        RackMap<String> rm = new RackMap<String>();

        rm.put("/a/b/c", "/a/b/c");
        rm.put("/a/d/e", "/a/d/e");

        Assert.assertEquals("Return closest leaf node", "/a/b/c", rm.get("/a/b/d"));
        rm.remove("/a/b/c");
        Assert.assertEquals("Return closest leaf node", "/a/d/e", rm.get("/a/b/d"));
        rm.remove("/a/d/e");
        Assert.assertEquals("Return closest leaf node", null, rm.get("/a/b/d"));
    }

    @Test
    public void testVariousRackIDLen() {
        RackMap<String> rm = new RackMap<String>();

        rm.put("/a", "/a");
        rm.put("/aa", "/aa");
        rm.put("/aaa", "/aaa");

        Assert.assertEquals("Inserted key/value should exist", "/a", rm.get("/a"));
        Assert.assertEquals("Inserted key/value should exist", "/aa", rm.get("/aa"));
        Assert.assertEquals("Inserted key/value should exist", "/aaa", rm.get("/aaa"));
    }
}
