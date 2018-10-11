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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStoreUtils.getAzureConfig;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStoreUtils.isAzureConfigured;
import static org.junit.Assume.assumeTrue;

import java.util.Properties;
import java.util.Set;

import javax.jcr.RepositoryException;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.blob.cloud.AbstractCloudDataStoreTest;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link AzureDataStore} with AzureDataStore and local cache on.
 * It requires to pass azure config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link AzureDataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/azure.properties. Sample azure properties located at
 * src/test/resources/azure.properties
 */
public class TestAzureDS extends AbstractCloudDataStoreTest {

    protected static final Logger LOG = LoggerFactory.getLogger(TestAzureDS.class);
    private static final Set<String> createdContainers = Sets.newHashSet();

    @BeforeClass
    public static void assumptions() {
        assumeTrue(isAzureConfigured());
    }

    @AfterClass
    public static void cleanupAllContainers() {
        for (final String container : createdContainers) {
            try {
                AzureDataStoreUtils.deleteContainer(container);
            }
            catch (Exception ignore) { }
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        createdContainers.add(container);
    }

    @Override
    protected DataStore createDataStore() throws RepositoryException {
        DataStore azureds = null;
        try {
            azureds = AzureDataStoreUtils.getAzureDataStore(props, dataStoreDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep(1000);
        return azureds;
    }

    /**---------- Skipped -----------**/
    @Override
    public void testUpdateLastModifiedOnAccess() {
    }

    @Override
    public void testDeleteAllOlderThan() {
    }


    @Override
    protected boolean isDataStoreConfigured() {
        return isAzureConfigured();
    }

    @Override
    protected Properties getDataStoreConfig(@NotNull final String containerName) {
        Properties props = getAzureConfig();
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);
        return props;
    }

    @Override
    protected void deleteContainer(@NotNull final String containerName) throws Exception {
        AzureDataStoreUtils.deleteContainer(containerName);
    }
}
