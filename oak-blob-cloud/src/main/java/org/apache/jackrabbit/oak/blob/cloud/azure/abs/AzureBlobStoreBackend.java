/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.abs;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.core.data.*;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.Utils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class AzureBlobStoreBackend implements SharedAzureBlobStoreBackend {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);

    private static final String KEY_PREFIX = "dataStore_";

    private static final String META_DIR_NAME = "META";

    private Properties properties;

    private ThreadPoolExecutor asyncWriteExecutor;

    private CloudBlobContainer azureContainer = null;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    protected CloudBlobContainer getAzureContainer() {
        return azureContainer;
    }

    /**
     * Get key from data identifier. Object is stored with key in Azure.
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
        } else if (key.contains(META_DIR_NAME)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }

    @Override
    public void init(CachingDataStore store, String homeDir, String config) throws DataStoreException {
        Properties initProps = null;
        //Check is configuration is already provided. That takes precedence
        //over config provided via file based config
        if(this.properties != null) {
            initProps = this.properties;
        }
        else {
            if(config == null){
                config = Utils.DEFAULT_CONFIG_FILE;
            }
            try {
                initProps = Utils.readConfig(config);
            }
            catch (IOException e) {
                throw new DataStoreException("Could not initialize Azure from "
                        + config, e);
            }
            this.properties = initProps;
        }
        init(store, homeDir, initProps);
    }

    public void init(CachingDataStore store, String homeDir, Properties props) throws DataStoreException {
        int writeThreads = 10;
        String writeThreadsStr = props.getProperty(AzureConstants.ABS_WRITE_THREADS);
        if (writeThreadsStr != null) {
            writeThreads = Integer.parseInt(writeThreadsStr);
        }
        int asyncWritePoolSize = 10;
        String maxConnsStr = props.getProperty(AzureConstants.ABS_MAX_CONNS);
        if (maxConnsStr != null) {
            asyncWritePoolSize = Integer.parseInt(maxConnsStr)
                    - writeThreads;
        }

        asyncWriteExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                asyncWritePoolSize, new NamedThreadFactory("azure-write-worker"));

        try {
            AzureConnectionString connectionString = new AzureConnectionString(
                    properties.getProperty(AzureConstants.AZURE_ACCOUNT_NAME, ""),
                    properties.getProperty(AzureConstants.AZURE_ACCOUNT_KEY, "")
            );
            CloudStorageAccount account = CloudStorageAccount.parse(connectionString.toString());
            CloudBlobClient client = account.createCloudBlobClient();
            azureContainer = client.getContainerReference(properties.getProperty(AzureConstants.ABS_CONTAINER_NAME));
        }
        catch (InvalidKeyException | URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
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

            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            return blob.openInputStream();
        }
        catch (URISyntaxException | StorageException  e) {
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public long getLength(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);

            blob.downloadAttributes();
            BlobProperties properties = blob.getProperties();
            return properties.getLength();
        }
        catch (URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public long getLastModified(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            blob.downloadAttributes();

            Map<String, String> metadata = blob.getMetadata();
            if (! metadata.containsKey("lastModified")) {
                return blob.getProperties().getLastModified().getTime();
            }
            return Long.parseLong(metadata.get("lastModified"));
        }
        catch (URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
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

        try {
            write(identifier, file, false, null);
        }
        catch (URISyntaxException | InvalidKeyException | StorageException | IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public void writeAsync(final DataIdentifier identifier,
                           final File file,
                           final AsyncUploadCallback callback) throws DataStoreException {

        // Fail early, so stack trace is in the calling thread
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }
        if (null == file) {
            throw new NullPointerException("file");
        }
        if (null == callback) {
            throw new NullPointerException("callback");
        }
        if (! file.exists()) {
            throw new DataStoreException(new FileNotFoundException(file.getName()));
        }

        asyncWriteExecutor.execute(new Runnable(){
            @Override
            public void run() {
                try {
                    write(identifier, file, true, callback);
                } catch (DataStoreException e) {
                    LOG.error("Could not upload [" + identifier + "], file[" + file
                            + "]", e);
                } catch (URISyntaxException | InvalidKeyException | StorageException | IOException e) {
                    // error
                }
            }
        });
    }

    private void write(final DataIdentifier identifier,
                       final File file,
                       boolean isAsyncUpload,
                       final AsyncUploadCallback callback)
            throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException, IOException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            String key = getKeyName(identifier);
            // Azure blob.upload() creates if it doesn't exist, updates if it does
            try {
                if (null == getAzureContainer()) {
                    throw new DataStoreException("No connection to Azure Blob Storage");
                }
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
                if (isAsyncUpload && null != callback) {
                    callback.onSuccess(new AsyncUploadResult(identifier, file));
                }
            } catch (URISyntaxException | StorageException | IOException e) {
                if (isAsyncUpload && null != callback) {
                    AsyncUploadResult result = new AsyncUploadResult(identifier, file);
                    result.setException(e);
                    callback.onAbort(result);
                }
                throw e;
            }
        }
        finally {
            if (null != contextClassLoader) {
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
    public boolean exists(DataIdentifier identifier, boolean touch) throws DataStoreException {
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            boolean blobExists = blob.exists();
            if (blobExists && touch) {
                touch(identifier, new DateTime().getMillis());
            }
            return blobExists;
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
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        return exists(identifier, false);
    }

    @Override
    public void touch(DataIdentifier identifier, long minModifiedDate) throws DataStoreException {
        if (minModifiedDate > 0) {
            String key = getKeyName(identifier);
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                if (null == getAzureContainer()) {
                    throw new DataStoreException("No connection to Azure Blob Storage");
                }
                CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);

                // ABS has a lastModified property that you can get via
                // blob.getProperties().getLastModified().  However, you
                // cannot modify this property directly.  You have to re-upload
                // the entire blob in order to modify this property.
                //
                // Instead we can manage this ourselves in a metadata property
                // we control, but then it won't match whatever gets displayed
                // in the Azure console.
                blob.downloadAttributes();
                HashMap<String, String> metadata = blob.getMetadata();
                long lastModified = Long.parseLong(metadata.get("lastModified"));
                if (minModifiedDate > lastModified) {
                    metadata.put("lastModified", Long.toString(minModifiedDate));
                    blob.setMetadata(metadata);
                    blob.uploadMetadata();
                }
            } catch (Exception e) {
                throw new DataStoreException(e);
            } finally {
                if (null != contextClassLoader) {
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
            }
        }
    }

    @Override
    public void touchAsync(final DataIdentifier identifier, final long minModifiedDate, final AsyncTouchCallback callback) throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (callback == null) {
                throw new IllegalArgumentException("callback parameter cannot be null in touchAsync");
            }
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            asyncWriteExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        touch(identifier, minModifiedDate);
                        callback.onSuccess(new AsyncTouchResult(identifier));
                    }
                    catch (DataStoreException e) {
                        AsyncTouchResult result = new AsyncTouchResult(identifier);
                        result.setException(e);
                        callback.onFailure(result);
                    }
                }
            });
        } catch (Exception e) {
            if (callback != null) {
                callback.onAbort(new AsyncTouchResult(identifier));
            }
            throw new DataStoreException("Cannot touch the record "
                    + identifier.toString(), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close() throws DataStoreException {
        asyncWriteExecutor.shutdownNow();
    }

    @Override
    public Set<DataIdentifier> deleteAllOlderThan(long timestamp) throws DataStoreException {
        final Set<DataIdentifier> deletedIdentifiers = Sets.newHashSet();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            for (ListBlobItem item : getAzureContainer().listBlobs()) {
                if (item instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) item;
                    if (blob.getProperties().getLastModified().getTime() < timestamp) {
                        blob.delete();
                        deletedIdentifiers.add(new DataIdentifier(getIdentifierName(blob.getName())));
                    }
                }
            }
        }
        catch (StorageException e) {
            throw new DataStoreException(e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return deletedIdentifiers;
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            blob.delete(); // TODO: Should we use blob.deleteIfExists(), and do nothing if identifier is invalid?
        } catch (Exception e) {
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
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);

            blob.upload(input, -1);
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
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            blob.upload(new FileInputStream(input), input.length());
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
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
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
            if (null != getAzureContainer()) {
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
        }
        catch (URISyntaxException e) {
            // TODO: error message
        }
        catch (StorageException e) {
            // TODO: error message
        }
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
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            CloudBlockBlob blob = metaDir.getBlockBlobReference(name);
            return blob.deleteIfExists();
        } catch (Exception e) { }
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
            if (null != getAzureContainer()) {
                CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
                for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                    if (item instanceof CloudBlob) {
                        ((CloudBlob)item).deleteIfExists();
                    }
                }
            }
        } catch (Exception e) { }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        return new RecordsIterator<DataRecord>(
                new Function<CloudBlob, DataRecord>() {
                    @Override
                    public DataRecord apply(CloudBlob input) {
                        return new AzureBlobStoreDataRecord(input,
                                getIdentifierName(input.getName()),
                                input.getProperties().getLastModified().getTime(),
                                input.getProperties().getLength(),
                                true);
                    }
                }
        );
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
            if (null == getAzureContainer()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            blob.downloadAttributes();
            return new AzureBlobStoreDataRecord(blob,
                    getIdentifierName(blob.getName()),
                    blob.getProperties().getLastModified().getTime(),
                    blob.getProperties().getLength(),
                    true);
        } catch (Exception e) {
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
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
            if (null != getAzureContainer()) {
                for (ListBlobItem item : getAzureContainer().listBlobs()) {
                    if (item instanceof CloudBlob) {
                        items.add((CloudBlob)item);
                    }
                }
            }
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
