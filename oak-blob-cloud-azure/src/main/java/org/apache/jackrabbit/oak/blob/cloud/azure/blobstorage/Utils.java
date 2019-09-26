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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.common.credentials.SharedKeyCredential;
import org.jetbrains.annotations.NotNull;

public final class Utils {

    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private Utils() {
    }

//    /**
//     * Create CloudBlobClient from properties.
//     *
//     * @param connectionString connectionString to configure @link {@link CloudBlobClient}
//     * @return {@link CloudBlobClient}
//     */
//    public static CloudBlobClient getBlobClient(final String connectionString) throws URISyntaxException, InvalidKeyException {
//        CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
//        CloudBlobClient client = account.createCloudBlobClient();
//        return client;
//    }

//    public static CloudBlobContainer getBlobContainer(final String connectionString, final String containerName) throws DataStoreException {
//        try {
//            CloudBlobClient client = Utils.getBlobClient(connectionString);
//            return client.getContainerReference(containerName);
//        } catch (InvalidKeyException | URISyntaxException | StorageException e) {
//            throw new DataStoreException(e);
//        }
//    }

    /**
     * Create a {@link ContainerClient} from Azure connection information.
     *
     * @param accountName The name of the storage account to connect to.
     * @param accountKey The account key used to authenticate.
     * @param containerName The name of the storage container to use.
     * @return A {@link ContainerClient} for interaction with the cloud storage.
     */
    public static ContainerClient getBlobContainer(@NotNull final String accountName,
                                                   @NotNull final String accountKey,
                                                   @NotNull final String containerName) {
        SharedKeyCredential credential = new SharedKeyCredential(accountName, accountKey);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s", getDefaultBlobStorageDomain(accountName)))
                .credential(credential)
                .buildClient();
        return blobServiceClient.getContainerClient(containerName);
    }

    public static String getDefaultBlobStorageDomain(@NotNull final String accountName) {
        return String.format("%s.blob.core.windows.net", accountName);
    }

    public static void setProxyIfNeeded(final Properties properties) {
//        String proxyHost = properties.getProperty(AzureConstants.PROXY_HOST);
//        String proxyPort = properties.getProperty(AzureConstants.PROXY_PORT);
//
//        if (!Strings.isNullOrEmpty(proxyHost) &&
//            Strings.isNullOrEmpty(proxyPort)) {
//            int port = Integer.parseInt(proxyPort);
//            SocketAddress proxyAddr = new InetSocketAddress(proxyHost, port);
//            Proxy proxy = new Proxy(Proxy.Type.HTTP, proxyAddr);
//            OperationContext.setDefaultProxy(proxy);
//        }
    }

//    public static RetryPolicy getRetryPolicy(final String maxRequestRetry) {
//        int retries = PropertiesUtil.toInteger(maxRequestRetry, -1);
//        if (retries < 0) {
//            return null;
//        }
//        if (retries == 0) {
//            return new RetryNoRetry();
//        }
//        return new RetryExponentialRetry(RetryPolicy.DEFAULT_CLIENT_BACKOFF, retries);
//    }


//    public static String getConnectionStringFromProperties(Properties properties) {
//
//        String sasUri = properties.getProperty(AzureConstants.AZURE_SAS, "");
//        String blobEndpoint = properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "");
//        String connectionString = properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, "");
//
//        if (!connectionString.isEmpty()) {
//            return connectionString;
//        }
//
//        if (!sasUri.isEmpty()) {
//            return getConnectionStringForSas(sasUri, blobEndpoint);
//        }
//
//        return getConnectionString(
//            properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, ""),
//            properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, ""));
//    }
//
//    private static String getConnectionStringForSas(String sasUri, String blobEndpoint) {
//        return String.format("BlobEndpoint=%s;SharedAccessSignature=%s", blobEndpoint, sasUri);
//    }
//
//    public static String getConnectionString(final String accountName, final String accountKey) {
//        return String.format(
//            "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s",
//            accountName,
//            accountKey
//        );
//    }

    /**
     * Read a configuration properties file. If the file name ends with ";burn",
     * the file is deleted after reading.
     *
     * @param fileName the properties file name
     * @return the properties
     * @throws java.io.IOException if the file doesn't exist
     */
    public static Properties readConfig(String fileName) throws IOException {
        if (!new File(fileName).exists()) {
            throw new IOException("Config file not found. fileName=" + fileName);
        }
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(fileName);
            prop.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return prop;
    }
}
