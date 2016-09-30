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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
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

public class AzureBlobStoreBackend implements Backend {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);

    private static final String KEY_PREFIX = "dataStore_";

    private static final String META_KEY_PREFIX = "META/";

    private Properties properties;

    private ThreadPoolExecutor asyncWriteExecutor;

    private Optional<CloudBlobContainer> azureContainer = Optional.absent();

    public void setProperties(final Properties properties) {
        this.properties = properties;
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
        } else if (key.contains(META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }

    @Override
    public void init(CachingDataStore store, String homeDir, String config) throws DataStoreException {
        Properties initProps = null;
        //Check is configuration is already provided. That takes precedence
        //over config provided via file based config
        if(this.properties != null){
            initProps = this.properties;
        } else {
            if(config == null){
                config = Utils.DEFAULT_CONFIG_FILE;
            }
            try{
                initProps = Utils.readConfig(config);
            }catch(IOException e){
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
            azureContainer = Optional.of(client.getContainerReference(properties.getProperty(AzureConstants.ABS_CONTAINER_NAME)));
        }
        catch (InvalidKeyException | URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);

            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);
            blob.download(out);
            // TODO: This blocks which could be bad for large
            // assets.  We may need to rethink this.
            return in;
        }
        catch (URISyntaxException | StorageException | IOException e) {
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public long getLength(DataIdentifier identifier) throws DataStoreException {
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);

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
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);
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
        if (null == callback) {
            throw new IllegalArgumentException("callback parameter cannot be null in asyncUpload");
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
            // Check to see if this blob is already in the data store
            // by retrieving metadata about the blob, and checking if
            // it matches what we think we know about the blob.
            //
            // If the blob exists - S3Backend appears to copy the
            // object to itself, presumably to update the metadata.
            //
            // Otherwise we upload the object.
            try {
                if (! azureContainer.isPresent()) {
                    throw new DataStoreException("No connection to Azure Blob Storage");
                }
                CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);
                blob.upload(new FileInputStream(file), file.length());
                long lastModified = blob.getProperties().getLastModified().getTime();
                Map<String, String> metadata = Maps.newHashMap();
                metadata.put("lastModified", Long.toString(lastModified));
                if (null != callback) {
                    callback.onSuccess(new AsyncUploadResult(identifier, file));
                }
            } catch (URISyntaxException | StorageException | IOException e) {
                if (null != callback) {
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
            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);
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
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);

            // ABS has a lastModified property that you can get via
            // blob.getProperties().getLastModified().  However, you
            // cannot modify this property directly.  You have to re-upload
            // the entire blob in order to modify this property.
            //
            // Instead we can manage this ourselves in a metadata property
            // we control, but then it won't match whatever gets displayed
            // in the Azure console.
            HashMap<String, String> metadata = blob.getMetadata();
            metadata.put("lastModified", Long.toString(minModifiedDate));
            blob.setMetadata(metadata);
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
            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            for (ListBlobItem item : azureContainer.get().listBlobs()) {
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
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            if (! azureContainer.isPresent()) {
                throw new DataStoreException("No connection to Azure Blob Storage");
            }
            CloudBlockBlob blob = azureContainer.get().getBlockBlobReference(key);
            blob.deleteIfExists();
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
            if (azureContainer.isPresent()) {
                for (ListBlobItem item : azureContainer.get().listBlobs()) {
                    if (item instanceof CloudBlob) {
                        items.add((CloudBlob)item);
                    }
                }
            }
        }
    }
}
