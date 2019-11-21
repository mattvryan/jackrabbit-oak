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
package org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.base.Strings;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.FixtureUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fixture for AzureDataStore based on an azure.properties config file. It creates
 * a new temporary Azure Blob Container for each DataStore created.
 *
 * Note: when using this, it's highly recommended to reuse the NodeStores across multiple tests (using
 * {@link org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest#AbstractRepositoryTest(NodeStoreFixture, boolean) AbstractRepositoryTest(fixture, true)})
 * otherwise it will be slower and can lead to out of memory issues if there are many tests.
 *
 * <p>
 * Test buckets are named "direct-binary-test-...". If some did not get cleaned up, you can
 * list them using the aws cli with this command:
 * <pre>
 *     az storage container list --output table | grep direct-binary-test-
 * </pre>
 *
 * And after checking, delete them all in one go with this command:
 * <pre>
 *     az storage container list --output table | grep direct-binary-test- | cut -d " " -f 1 | xargs -n 1 -I {} sh -c 'az storage container delete -n {}'
 * </pre>
 */
public class AzureDataStoreFixture implements DataStoreFixture {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nullable
    private final Properties azProps;
    private Map<DataStore, CloudBlobContainer> containers = new HashMap<>();

    public AzureDataStoreFixture() {
        azProps = FixtureUtils.loadDataStoreProperties("azure.config", "azure.properties", ".azure");
    }

    @Override
    public boolean isAvailable() {
        if (azProps == null) {
            log.warn("Skipping Azure DataStore fixture because no AZ properties file was found given by " +
                "'azure.config' system property or named 'azure.properties' or '~/.azure/azure.properties'.");
            return false;
        }
        return true;
    }

    private CloudBlobContainer getBlobContainer(@NotNull final Properties properties, @NotNull final String containerName) throws DataStoreException {
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, null);
        if (Strings.isNullOrEmpty(accountName)) {
            throw new DataStoreException(String.format("%s - %s '%s'",
                    "Unable to initialize Azure Data Store",
                    "missing required configuration parameter",
                    AzureConstants.AZURE_STORAGE_ACCOUNT_NAME
            ));
        }

        return getBlobContainer(getConnectionStringFromProperties(properties), containerName);
    }

    private String getConnectionStringFromProperties(@NotNull final Properties properties) {
        String sasUri = properties.getProperty(AzureConstants.AZURE_SAS, "");
        String blobEndpoint = properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "");
        String connectionString = properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, "");

        if (!connectionString.isEmpty()) {
            return connectionString;
        }

        if (!sasUri.isEmpty()) {
            return getConnectionStringForSas(sasUri, blobEndpoint);
        }

        return getConnectionString(
                properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, ""),
                properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, ""));

    }

    private static CloudBlobClient getBlobClient(final String connectionString) throws URISyntaxException, InvalidKeyException {
        CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
        CloudBlobClient client = account.createCloudBlobClient();
        return client;
    }

    private static CloudBlobContainer getBlobContainer(final String connectionString, final String containerName) throws DataStoreException {
        try {
            CloudBlobClient client = getBlobClient(connectionString);
            return client.getContainerReference(containerName);
        } catch (InvalidKeyException | URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        }
    }

    private static String getConnectionStringForSas(String sasUri, String blobEndpoint) {
        return String.format("BlobEndpoint=%s;SharedAccessSignature=%s", blobEndpoint, sasUri);
    }

    private static String getConnectionString(final String accountName, final String accountKey) {
        return String.format(
                "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s",
                accountName,
                accountKey
        );
    }

    @NotNull
    @Override
    public DataStore createDataStore() {
        if (!isAvailable() || azProps == null) {
            throw new AssertionError("createDataStore() called but this fixture is not available");
        }

        // Create a temporary container that will be removed at test completion
        String containerName = "direct-binary-test-" + UUID.randomUUID().toString();

        log.info("Creating Azure test blob container {}", containerName);

        try {
            CloudBlobContainer container = getBlobContainer(azProps, containerName);
            container.createIfNotExists();

            // create new properties since azProps is shared for all created DataStores
            Properties clonedAzProps = new Properties();
            clonedAzProps.putAll(azProps);
            clonedAzProps.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container.getName());

            // setup Oak DS
            AzureDataStore dataStore = new AzureDataStore();
            dataStore.setProperties(clonedAzProps);
            dataStore.setStagingSplitPercentage(0);

            containers.put(dataStore, container);
            return dataStore;

        } catch (DataStoreException | StorageException e) {
            throw new AssertionError("Azure DataStore fixture fails because of issue with Azure config or connection", e);
        }
    }

    @Override
    public void dispose(DataStore dataStore) {
        if (dataStore == null) {
            return;
        }

        try {
            dataStore.close();
        } catch (DataStoreException e) {
            log.warn("Issue while disposing DataStore", e);
        }

        CloudBlobContainer container = containers.get(dataStore);
        if (container != null) {
            log.info("Removing Azure test blob container {}", container.getName());
            try {
                // For Azure, you can just delete the container and all
                // blobs it in will also be deleted
                container.delete();
            } catch (StorageException e) {
                log.warn("Unable to delete Azure Blob container {}", container.getName());
            }

            containers.remove(dataStore);
        }
    }
}
