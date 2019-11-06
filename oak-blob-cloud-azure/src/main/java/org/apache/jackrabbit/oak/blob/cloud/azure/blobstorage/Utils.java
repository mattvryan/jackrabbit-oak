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
import com.azure.storage.common.credentials.SASTokenCredential;
import com.azure.storage.common.credentials.SharedKeyCredential;
import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.jetbrains.annotations.NotNull;

public final class Utils {

    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private Utils() {
    }

    /**
     * Create a {@link ContainerClient} from the provided properties (configuration).
     *
     * Tries to create a client using the account name (which must be provided
     * in the {@link Properties}) and the account key, if provided.  If no
     * account key is provided but a shared access signature is provided, a
     * client will be created using the shared access signature instead.  If
     * neither is provided, an exception is thrown indicating the configuration
     * error.
     *
     * This method does not attempt to create a valid shared access signature
     * using any existing configuration information (e.g. account name and key).
     * The shared access signature, if provided, must already be provisioned for
     * the storage service and must be valid to get a working client.
     *
     * @param properties Properties to configure the client connection.
     * @param containerName The name of the storage container to use.
     * @return A {@link ContainerClient} for interaction with the cloud storage.
     * @throws DataStoreException
     */
    public static ContainerClient getBlobContainer(@NotNull final Properties properties, @NotNull final String containerName)
            throws DataStoreException {
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, null);
        if (Strings.isNullOrEmpty(accountName)) {
            throw new DataStoreException(String.format("%s - %s '%s'",
                    "Unable to initialize Azure Data Store",
                    "missing required configuration parameter",
                    AzureConstants.AZURE_STORAGE_ACCOUNT_NAME
            ));
        }

        String accountKey = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, null);
        if (Strings.isNullOrEmpty(accountKey)) {
            String azureSAS = properties.getProperty(AzureConstants.AZURE_SAS, null);
            if (Strings.isNullOrEmpty(azureSAS)) {
                throw new DataStoreException(String.format("%s - %s '%s' or '%s'",
                        "Unable to initialize Azure Data Store",
                        "missing required configuration parameter",
                        AzureConstants.AZURE_STORAGE_ACCOUNT_KEY,
                        AzureConstants.AZURE_SAS
                ));
            }
            else {
                return getBlobContainerWithSharedAccessSignature(accountName, azureSAS, containerName);
            }
        }
        else {
            return getBlobContainerWithAccountKey(accountName, accountKey, containerName);
        }
    }

    /**
     * Create a {@link ContainerClient} using a shared access signature.
     *
     * This only uses the provided information to create a client from the
     * provided shared access signature.  It does not attempt to create a valid
     * shared access signature using existing connection information.
     *
     * If no shared access signature is provided in the configuration, instead
     * use {@link #getBlobContainerWithAccountKey(String, String, String)}.
     *
     * @param accountName The name of the Azure Blob Storage account.
     * @param azureSAS The already-provisioned shared access signature string.
     * @param containerName The name of the storage container to use.
     * @return A {@link ContainerClient} for interaction with the cloud storage.
     */
    public static ContainerClient getBlobContainerWithSharedAccessSignature(@NotNull final String accountName,
                                                                            @NotNull final String azureSAS,
                                                                            @NotNull final String containerName) {
        SASTokenCredential credential = SASTokenCredential.fromSASTokenString(azureSAS);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(String.format("https://%s", getDefaultBlobStorageDomain(accountName)))
                .credential(credential)
                .buildClient();
        return blobServiceClient.getContainerClient(containerName);
    }

    /**
     * Create a {@link ContainerClient} from Azure connection information.
     *
     * @param accountName The name of the storage account to connect to.
     * @param accountKey The account key used to authenticate.
     * @param containerName The name of the storage container to use.
     * @return A {@link ContainerClient} for interaction with the cloud storage.
     */
    public static ContainerClient getBlobContainerWithAccountKey(@NotNull final String accountName,
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
