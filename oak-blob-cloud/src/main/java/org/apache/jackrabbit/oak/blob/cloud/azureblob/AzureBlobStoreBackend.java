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

package org.apache.jackrabbit.oak.blob.cloud.azureblob;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

public class AzureBlobStoreBackend extends AbstractSharedBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);

    private static final String BACKEND_NAME = "azureblobstore";

    private static final String META_DIR_NAME = "META";
    private static final String META_KEY_PREFIX = META_DIR_NAME + "/";

    private Properties properties;
    private String containerName;
    private CloudBlobContainer azureContainer;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    protected CloudBlobContainer getAzureContainer() throws DataStoreException {
        if (null == azureContainer) {
            throw new DataStoreException("No connection to Azure Blob Storage");
        }
        return azureContainer;
    }

    @Override
    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            LOG.debug("init");

            if (null == properties) {
                try {
                    properties = Utils.readConfig(Utils.DEFAULT_CONFIG_FILE);
                }
                catch (IOException e) {
                    throw new DataStoreException("Unable to initialize Azure Data Store from " + Utils.DEFAULT_CONFIG_FILE, e);
                }
            }

            try {
                containerName = (String) properties.get(AzureConstants.ABS_CONTAINER_NAME);
                AzureConnectionString connectionString = new AzureConnectionString(
                        properties.getProperty(AzureConstants.AZURE_ACCOUNT_NAME, ""),
                        properties.getProperty(AzureConstants.AZURE_ACCOUNT_KEY, "")
                );
                CloudStorageAccount account = CloudStorageAccount.parse(connectionString.toString());
                CloudBlobClient client = account.createCloudBlobClient();
                azureContainer = client.getContainerReference(containerName);
            }
            catch (InvalidKeyException | URISyntaxException | StorageException e) {
                throw new DataStoreException(e);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            return getAzureContainer().getBlockBlobReference(key).openInputStream();
        }
        catch (URISyntaxException | StorageException e) {
            throw new DataStoreException(String.format("Couldn't read blob %s in container %s", key, containerName), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }
        if (null == file) {
            throw new NullPointerException("file");
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            String key = getKeyName(identifier);

            // Azure blob.upload() creates if it doesn't exist, updates if it does
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            HashMap<String, String> metadata = Maps.newHashMap();
            if (blob.exists()) {
                metadata = blob.getMetadata();
            }
            blob.upload(new FileInputStream(file), file.length());
            long lastModified = blob.getProperties().getLastModified().getTime();
            metadata.put("lastModified", Long.toString(lastModified));
            blob.setMetadata(metadata);
            blob.uploadMetadata();
        }
        catch (URISyntaxException | StorageException | IOException e) {
            throw new DataStoreException(
                    String.format("Couldn't write blob %s in container %s",
                            getKeyName(identifier), containerName),
                    e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }

    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }

        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (blob.exists()) {
                blob.downloadAttributes();
                return new AzureBlobStoreDataRecord(blob,
                        getIdentifierName(blob.getName()),
                        blob.getProperties().getLastModified().getTime(),
                        blob.getProperties().getLength());
            } else {
                throw new DataStoreException("Couldn't find blob");
            }
        }
        catch (URISyntaxException | StorageException e) {
            throw new DataStoreException(String.format("Couldn't retrieve blob %s in container %s", key, containerName), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return new RecordsIterator<DataIdentifier>(
                new Function<CloudBlob, DataIdentifier>() {
                    @Override
                    public DataIdentifier apply(CloudBlob input) {
                        return new DataIdentifier(getIdentifierName(input.getName()));
                    }
                }
        );
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        return new RecordsIterator<DataRecord>(
                new Function<CloudBlob, DataRecord>() {
                    @Override
                    public DataRecord apply(CloudBlob input) {
                        return new AzureBlobStoreDataRecord(input,
                                getIdentifierName(input.getName()),
                                input.getProperties().getLastModified().getTime(),
                                input.getProperties().getLength());
                    }
                }
        );
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            return getAzureContainer().getBlockBlobReference(key).exists();
        }
        catch (Exception e) {
            throw new DataStoreException(e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close() throws DataStoreException {
        //asyncWriteExecutor.shutdownNow();
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            getAzureContainer().getBlockBlobReference(key).deleteIfExists();
            // TODO: Should we use blob.delete() instead? What happens if identifier is invalid?
        } catch (URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(InputStream input, String name) throws DataStoreException {
        if (null == input) {
            throw new NullPointerException("input");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name");
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(input, name, -1L);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        if (null == input) {
            throw new NullPointerException("input");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name");
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(new FileInputStream(input), name, input.length());
        }
        catch (IOException e) {
            throw new DataStoreException(e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private void addMetadataRecordImpl(final InputStream input, String name, long recordLength) throws DataStoreException {
        try {
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            blob.upload(input, recordLength);
        }
        catch (URISyntaxException | StorageException | IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            blob.downloadAttributes();
            long lastModified = blob.getProperties().getLastModified().getTime();
            long length = blob.getProperties().getLength();
            return new AzureBlobStoreDataRecord(getAzureContainer(), name, lastModified, length, true);
        }
        catch (Exception e) {
            return null;
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }

        final List<DataRecord> records = Lists.newArrayList();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) item;
                    records.add(new AzureBlobStoreDataRecord(getAzureContainer(),
                            blob.getName(),
                            blob.getProperties().getLastModified().getTime(),
                            blob.getProperties().getLength(),
                            true));
                }
            }
        }
        catch (URISyntaxException | StorageException | DataStoreException e) { }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return records;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            return blob.deleteIfExists();
        }
        catch (URISyntaxException | StorageException | DataStoreException e) { }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    ((CloudBlob)item).deleteIfExists();
                }
            }
        }
        catch (URISyntaxException | StorageException | DataStoreException e) { }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Get key from data identifier. Object is stored with key in ADS.
     */
    private static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + Utils.DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    private static String getIdentifierName(String key) {
        if (!key.contains(Utils.DASH)) {
            return null;
        } else if (key.contains(META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }

    private static String getMetadataRecordName(final String name) {
        return META_KEY_PREFIX+name;
    }

    private class AzureConnectionString {
        final String protocol;
        final String accountName;
        final String accountKey;
        public AzureConnectionString(final String accountName, final String accountKey) {
            this(accountName, accountKey, "http");
        }
        public AzureConnectionString(final String accountName, final String accountKey, final String protocol) {
            this.accountName = accountName;
            this.accountKey = accountKey;
            this.protocol = protocol;
        }
        public String toString() {
            return String.format(
                    "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s",
                    protocol,
                    accountName,
                    accountKey
            );
        }
    }

    private class RecordsIterator<T> extends AbstractIterator<T> {
        final Function<CloudBlob, T> transformer;
        final Queue<CloudBlob> items = Lists.newLinkedList();
        boolean firstTimeThrough = true;

        public RecordsIterator (Function<CloudBlob, T> transformer) {
            this.transformer = transformer;
        }

        @Override
        protected T computeNext() {
            if (items.isEmpty() && firstTimeThrough) {
                loadItems();
            }
            if (! items.isEmpty()) {
                return transformer.apply(items.remove());
            }
            return endOfData();
        }

        private void loadItems() {
            firstTimeThrough = false;
            try {
                CloudBlobContainer container = getAzureContainer();
                if (null != container) {
                    for (ListBlobItem item : container.listBlobs()) {
                        if (item instanceof CloudBlob) {
                            items.add((CloudBlob) item);
                        }
                    }
                }
            }
            catch (DataStoreException e) { }
        }
    }

    static class AzureBlobStoreDataRecord implements DataRecord {
        final Optional<CloudBlob> blob;
        final Optional<CloudBlobContainer> container;
        final DataIdentifier identifier;
        final long lastModified;
        final long length;
        final boolean isMeta;

        public AzureBlobStoreDataRecord(final CloudBlobContainer container, final String key, long lastModified, long length) {
            this(container, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(final CloudBlobContainer container, final String key, long lastModified, long length, final boolean isMeta) {
            this.blob = Optional.absent();
            this.container = Optional.of(container);
            this.identifier = new DataIdentifier(key);
            this.lastModified = lastModified;
            this.length = length;
            this.isMeta = isMeta;
        }

        public AzureBlobStoreDataRecord(final CloudBlob blob, final String key, long lastModified, long length) {
            this(blob, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(final CloudBlob blob, final String key, long lastModified, long length, final boolean isMeta) {
            this.blob = Optional.of(blob);
            this.container = Optional.absent();
            this.identifier = new DataIdentifier(key);
            this.lastModified = lastModified;
            this.length = length;
            this.isMeta = isMeta;
        }

        @Override
        public DataIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public String getReference() {
            return identifier.toString();
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            try {
                CloudBlob blob;
                if (this.blob.isPresent()) {
                    blob = this.blob.get();
                }
                else if (isMeta) {
                    CloudBlobDirectory metaDir = container.get().getDirectoryReference(META_DIR_NAME);
                    blob = metaDir.getBlockBlobReference(identifier.toString());
                }
                else {
                    blob = container.get().getBlockBlobReference(identifier.toString());
                }
                return blob.openInputStream();
            }
            catch (Exception e) {
                throw new DataStoreException(e);
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }
    }
}
