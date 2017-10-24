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

package org.apache.jackrabbit.oak.blob.composite.config;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

public class DelegateDataStoreConfigTest {

    @Test
    public void testCreateFromString() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1,prop1:val1"
        );

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
        assertEquals("local1", spec.get().getRole());
        assertEquals("val1", spec.get().getProperty("prop1"));
        assertNull(spec.get().getProperty("homeDir"));
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testCreateFromStringFileDataStore() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1"
        );

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromStringS3DataStore() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:S3DataStore,role:cloud1"
        );

        assertTrue(spec.isPresent());
        assertEquals("S3DataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromStringAzureDataStore() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:AzureDataStore,role:cloud1"
        );

        assertTrue(spec.isPresent());
        assertEquals("AzureDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromStringNoDataStoreNameFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "role:local1,prop1:val1"
        );

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringInvalidDataStoreNameFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:InvalidDataStore,role:local1"
        );

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringEmptyStringFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString("");

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringNullStringFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(null);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringNoRoleFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,prop1:val1"
        );

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringSetsHomeDir() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1,homeDir:/path/to/home"
        );

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperty("homeDir"));
    }

    @Test
    public void testCreateFromStringSetsHomeDirFromPath() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1,path:/path/to/home"
        );

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperty("path"));
        assertEquals("/path/to/home", spec.get().getProperty("homeDir"));
    }

    @Test
    public void testCreateFromStringSetsHomeDirFromRepositoryHome() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1,repository.home:/path/to/home"
        );

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperty("repository.home"));
        assertEquals("/path/to/home", spec.get().getProperty("homeDir"));
    }

    @Test
    public void testCreateFromStringSetReadOnly() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1,readOnly:true"
        );
        assertTrue(spec.isPresent());
        assertTrue(spec.get().isReadOnly());

        spec = DelegateDataStoreConfig.createFromString(
                "dataStoreName:FileDataStore,role:local1,readOnly:false"
        );
        assertTrue(spec.isPresent());
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testCreateFromProperties() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "local1");
        p.put("prop1", "val1");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
        assertEquals("local1", spec.get().getRole());
        assertEquals("val1", spec.get().getProperty("prop1"));
        assertNull(spec.get().getProperty("homeDir"));
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testCreateFromPropertiesFileDataStore() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "local1");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromPropertiesS3DataStore() {
        Properties p = new Properties();
        p.put("dataStoreName", "S3DataStore");
        p.put("role", "cloud1");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("S3DataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromPropertiesAzureDataStore() {
        Properties p = new Properties();
        p.put("dataStoreName", "AzureDataStore");
        p.put("role", "cloud1");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("AzureDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromPropertiesNoDataStoreNameFails() {
        Properties p = new Properties();
        p.put("role", "local1");
        p.put("prop1", "val1");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesInvalidDataStoreNameFails() {
        Properties p = new Properties();
        p.put("dataStoreName", "InvalidDataStore");
        p.put("role", "local1");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesEmptyPropertiesFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(new Properties());

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesNullPropertiesFails() {
        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(null);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesNoRoleFails() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesEmptyRoleFails() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesSetsHomeDir() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "local1");
        p.put("homeDir", "/path/to/home");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperty("homeDir"));
    }

    @Test
    public void testCreateFromPropertiesSetsHomeDirFromPath() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "local1");
        p.put("path", "/path/to/home");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperty("path"));
        assertEquals("/path/to/home", spec.get().getProperty("homeDir"));
    }

    @Test
    public void testCreateFromPropertiesSetsHomeDirFromRepositoryHome() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "local1");
        p.put("repository.home", "/path/to/home");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperty("repository.home"));
        assertEquals("/path/to/home", spec.get().getProperty("homeDir"));
    }

    @Test
    public void testCreateFromPropertiesSetReadOnly() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("role", "local1");
        p.put("readOnly", "true");

        Optional<DelegateDataStoreConfig> spec = DelegateDataStoreConfig.createFromProperties(p);
        assertTrue(spec.isPresent());
        assertTrue(spec.get().isReadOnly());

        p.put("readOnly", "false");
        spec = DelegateDataStoreConfig.createFromProperties(p);
        assertTrue(spec.isPresent());
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testToString() {
        for (String cfg : Lists.newArrayList(
                "dataStoreName:FileDataStore,role:local1",
                "dataStoreName:FileDataStore,role:local1,homeDir:/path/to/home",
                "dataStoreName:FileDataStore,role:local1,homeDir:/path/to/home,readOnly:true")) {
            assertEquals(cfg, DelegateDataStoreConfig.createFromString(cfg).get().toString());
        }
    }
}
