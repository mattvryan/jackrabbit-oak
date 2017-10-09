/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class CompositeDataStoreTest {
    private static Map<String, Object> oneFDSDelegateConfig = Maps.newHashMap();
    private static Map<String, Object> twoFDSDelegatesConfig = Maps.newHashMap();
    private static Map<String, Object> threeFDSDelegatesConfig = Maps.newHashMap();

    @BeforeClass
    public static void setupClass() {

    }

    @Test
    public void testCreateCompositeDataStore() {
        assertNotNull(new CompositeDataStore());
    }

    @Test
    public void testSetProperties() {

    }

    @Test
    public void testAddDelegate() {

    }

    @Test
    public void testAddTwoDelegates() {

    }

    @Test
    public void testAddThreeDelegates() {

    }

    @Test
    public void testAddDelegatesActivatedBefore() {

    }

    @Test
    public void testAddDelegatesActivatedAfter() {

    }

    @Test
    public void testRemoveDelegatesOnDeactivation() {

    }

    @Test
    public void testRemoveAndReaddDelegate() {

    }
}
