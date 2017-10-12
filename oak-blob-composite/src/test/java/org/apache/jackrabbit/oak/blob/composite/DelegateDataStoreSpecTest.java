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

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

public class DelegateDataStoreSpecTest {

    @Test
    public void testCreateFromString() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore,prop1:val1"
        );

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
        assertEquals("val1", spec.get().getProperties().get("prop1"));
        assertNull(spec.get().getProperties().get("homeDir"));
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testCreateFromStringFileDataStore() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore"
        );

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromStringS3DataStore() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:S3DataStore"
        );

        assertTrue(spec.isPresent());
        assertEquals("S3DataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromStringAzureDataStore() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:AzureDataStore"
        );

        assertTrue(spec.isPresent());
        assertEquals("AzureDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromStringNoDataStoreNameFails() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "prop1:val1"
        );

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringInvalidDataStoreNameFails() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:InvalidDataStore"
        );

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringEmptyStringFails() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString("");

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringNullStringFails() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(null);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromStringSetsHomeDir() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore,homeDir:/path/to/home"
        );

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperties().get("homeDir"));
    }

    @Test
    public void testCreateFromStringSetsHomeDirFromPath() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore,path:/path/to/home"
        );

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperties().get("path"));
        assertEquals("/path/to/home", spec.get().getProperties().get("homeDir"));
    }

    @Test
    public void testCreateFromStringSetsHomeDirFromRepositoryHome() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore,repository.home:/path/to/home"
        );

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperties().get("repository.home"));
        assertEquals("/path/to/home", spec.get().getProperties().get("homeDir"));
    }

    @Test
    public void testCreateFromStringSetReadOnly() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore,readOnly:true"
        );
        assertTrue(spec.isPresent());
        assertTrue(spec.get().isReadOnly());

        spec = DelegateDataStoreSpec.createFromString(
                "dataStoreName:FileDataStore,readOnly:false"
        );
        assertTrue(spec.isPresent());
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testCreateFromProperties() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("prop1", "val1");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
        assertEquals("val1", spec.get().getProperties().get("prop1"));
        assertNull(spec.get().getProperties().get("homeDir"));
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testCreateFromPropertiesFileDataStore() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("FileDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromPropertiesS3DataStore() {
        Properties p = new Properties();
        p.put("dataStoreName", "S3DataStore");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("S3DataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromPropertiesAzureDataStore() {
        Properties p = new Properties();
        p.put("dataStoreName", "AzureDataStore");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("AzureDataStore", spec.get().getDataStoreName());
    }

    @Test
    public void testCreateFromPropertiesNoDataStoreNameFails() {
        Properties p = new Properties();
        p.put("prop1", "val1");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesInvalidDataStoreNameFails() {
        Properties p = new Properties();
        p.put("dataStoreName", "InvalidDataStore");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesEmptyPropertiesFails() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(new Properties());

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesNullPropertiesFails() {
        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(null);

        assertFalse(spec.isPresent());
    }

    @Test
    public void testCreateFromPropertiesSetsHomeDir() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("homeDir", "/path/to/home");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperties().get("homeDir"));
    }

    @Test
    public void testCreateFromPropertiesSetsHomeDirFromPath() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("path", "/path/to/home");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperties().get("path"));
        assertEquals("/path/to/home", spec.get().getProperties().get("homeDir"));
    }

    @Test
    public void testCreateFromPropertiesSetsHomeDirFromRepositoryHome() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("repository.home", "/path/to/home");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);

        assertTrue(spec.isPresent());
        assertEquals("/path/to/home", spec.get().getProperties().get("repository.home"));
        assertEquals("/path/to/home", spec.get().getProperties().get("homeDir"));
    }

    @Test
    public void testCreateFromPropertiesSetReadOnly() {
        Properties p = new Properties();
        p.put("dataStoreName", "FileDataStore");
        p.put("readOnly", "true");

        Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(p);
        assertTrue(spec.isPresent());
        assertTrue(spec.get().isReadOnly());

        p.put("readOnly", "false");
        spec = DelegateDataStoreSpec.createFromProperties(p);
        assertTrue(spec.isPresent());
        assertFalse(spec.get().isReadOnly());
    }

    @Test
    public void testToString() {
        for (String cfg : Lists.newArrayList(
                "dataStoreName:FileDataStore",
                "dataStoreName:FileDataStore,homeDir:/path/to/home",
                "dataStoreName:FileDataStore,homeDir:/path/to/home,readOnly:true")) {
            assertEquals(cfg, DelegateDataStoreSpec.createFromString(cfg).get().toString());
        }
    }
}
