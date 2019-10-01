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

import static java.lang.Thread.currentThread;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.azure.core.http.rest.Response;
import com.azure.core.http.rest.VoidResponse;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobProperties;
import com.azure.storage.blob.BlobSASPermission;
import com.azure.storage.blob.BlockBlobClient;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockList;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.StorageException;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobStoreBackend extends AbstractSharedBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);
    private static final Logger LOG_STREAMS_DOWNLOAD = LoggerFactory.getLogger("oak.datastore.download.streams");
    private static final Logger LOG_STREAMS_UPLOAD = LoggerFactory.getLogger("oak.datastore.upload.streams");

    private static final String META_DIR_NAME = "META";
    private static final String META_KEY_PREFIX = META_DIR_NAME + "/";

    private static final String REF_KEY = "reference.key";

    private static final long BUFFERED_STREAM_THRESHHOLD = 1024 * 1024;
    static final long MIN_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 10; // 10MB
    static final long MAX_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 100; // 100MB
    static final long MAX_SINGLE_PUT_UPLOAD_SIZE = 1024 * 1024 * 256; // 256MB, Azure limit
    static final long MAX_BINARY_UPLOAD_SIZE = (long) Math.floor(1024L * 1024L * 1024L * 1024L * 4.75); // 4.75TB, Azure limit
    private static final int MAX_ALLOWABLE_UPLOAD_URIS = 50000; // Azure limit
    private static final int MAX_UNIQUE_RECORD_TRIES = 10;

    private Properties properties;
    private String containerName;
    private ContainerClient containerClient;

    private int concurrentRequestCount = 1;
    private int httpDownloadURIExpirySeconds = 0; // disabled by default
    private int httpUploadURIExpirySeconds = 0; // disabled by default
    private boolean createBlobContainer = true;
    private boolean presignedDownloadURIVerifyExists = true;

    private Cache<DataIdentifier, URI> httpDownloadURICache;

    private byte[] secret;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            LOG.debug("Started backend initialization");

            if (null == properties) {
                try {
                    properties = Utils.readConfig(Utils.DEFAULT_CONFIG_FILE);
                }
                catch (IOException e) {
                    throw new DataStoreException("Unable to initialize Azure Data Store from " + Utils.DEFAULT_CONFIG_FILE, e);
                }
            }

            try {
                Utils.setProxyIfNeeded(properties);

                createBlobContainer = PropertiesUtil.toBoolean(properties.getProperty(AzureConstants.AZURE_CREATE_CONTAINER), true);
                concurrentRequestCount = PropertiesUtil.toInteger(properties.get(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION), 1);
                LOG.info("Using concurrentRequestsPerOperation={}", concurrentRequestCount);
                presignedDownloadURIVerifyExists =
                        PropertiesUtil.toBoolean(properties.get(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS), true);

                containerName = properties.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, null);
                if (Strings.isNullOrEmpty(containerName)) {
                    throw new DataStoreException(String.format("%s - %s '%s'",
                            "Unable to initialize Azure Data Store",
                            "missing required configuration parameter",
                            AzureConstants.AZURE_BLOB_CONTAINER_NAME
                    ));
                }

                containerClient = Utils.getBlobContainer(properties, containerName);

                if (createBlobContainer && ! containerClient.exists()) {
                    VoidResponse rsp = containerClient.createWithResponse(null, null, null, Context.NONE);
                    if (rsp.statusCode() != 201) {
                        String errorMessage = String.format("%s - %s %d.  Container Name='%s'",
                                "Unable to initialize Azure Data Store",
                                "container does not exist and creation attempt failed with status",
                                rsp.statusCode(),
                                containerName
                        );
                        LOG.error(errorMessage);
                        throw new DataStoreException(errorMessage);
                    }
                    LOG.info("New container created. containerName={}", containerName);
                } else {
                    LOG.info("Reusing existing container. containerName={}", containerName);
                }
                LOG.debug("Backend initialized. duration={}",
                          +(System.currentTimeMillis() - start));

                // settings pertaining to DataRecordAccessProvider functionality
                String putExpiry = properties.getProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS);
                if (null != putExpiry) {
                    this.setHttpUploadURIExpirySeconds(Integer.parseInt(putExpiry));
                }
                String getExpiry = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS);
                if (null != getExpiry) {
                    this.setHttpDownloadURIExpirySeconds(Integer.parseInt(getExpiry));
                    String cacheMaxSize = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE);
                    if (null != cacheMaxSize) {
                        this.setHttpDownloadURICacheSize(Integer.parseInt(cacheMaxSize));
                    }
                    else {
                        this.setHttpDownloadURICacheSize(0); // default
                    }
                }
            }
            catch (Exception e) {
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
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());

            BlockBlobClient blob = containerClient.getBlockBlobClient(key);
            if (! blob.exists()) {
                throw new DataStoreException(String.format("Trying to read missing blob. identifier=%s", key));
            }

            InputStream is = blob.openInputStream();

            LOG.debug("Got input stream for blob. identifier={} duration={}", key, (System.currentTimeMillis() - start));
            if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                // Log message, with exception so we can get a trace to see where the call came from
                LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={}", key, new Exception());
            }
            return is;
        }
        catch (StorageException e) {
            LOG.info("Error reading blob. identifier=%s", key);
            throw new DataStoreException(String.format("Cannot read blob. identifier=%s", key), e);
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
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            long len = file.length();
            LOG.debug("Blob write started. identifier={} length={}", key, len);
            BlockBlobClient blob = containerClient.getBlockBlobClient(key);
            if (!blob.exists()) {
                boolean useBufferedStream = len < BUFFERED_STREAM_THRESHHOLD;
                final InputStream in = useBufferedStream  ? new BufferedInputStream(new FileInputStream(file)) : new FileInputStream(file);
                try {
                    blob.upload(in, len);
                    LOG.debug("Blob created. identifier={} length={} duration={} buffered={}", key, len, (System.currentTimeMillis() - start), useBufferedStream);
                    if (LOG_STREAMS_UPLOAD.isDebugEnabled()) {
                        // Log message, with exception so we can get a trace to see where the call came from
                        LOG_STREAMS_UPLOAD.debug("Binary uploaded to Azure Blob Storage - identifier={}", key, new Exception());
                    }
                } finally {
                    in.close();
                }
                return;
            }

            if (blob.getProperties().blobSize() != len) {
                throw new DataStoreException("Length Collision. identifier=" + key +
                                             " new length=" + len +
                                             " old length=" + blob.getProperties().blobSize());
            }
            LOG.trace("Blob already exists. identifier={} lastModified={}", key, blob.getProperties().lastModified().toString());
            //blob.startCopy(blob);
            Response rsp = blob.copyFromURLWithResponse(blob.getBlobUrl(),
                    null, null, null,
                    null, null, Context.NONE);
            //TODO: better way of updating lastModified (use custom metadata?)
            if (rsp.statusCode() < 400) {
                throw new DataStoreException(
                    String.format("Cannot update lastModified for blob. identifier=%s status=%d",
                                  key, rsp.statusCode()));
            }
            LOG.debug("Blob updated. identifier={} lastModified={} duration={}", key,
                      blob.getProperties().lastModified().toString(), (System.currentTimeMillis() - start));
        }
        catch (StorageException e) {
            LOG.info("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        }
        catch (IOException e) {
            LOG.debug("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            if (secret != null && secret.length != 0) {
                return secret;
            } else {
                byte[] key;
                // Try reading from the metadata folder if it exists
                key = readMetadataBytes(REF_KEY);
                if (key == null) {
                    key = super.getOrCreateReferenceKey();
                    addMetadataRecord(new ByteArrayInputStream(key), REF_KEY);
                    key = readMetadataBytes(REF_KEY);
                }
                secret = key;
                return secret;
            }
        } catch (IOException e) {
            throw new DataStoreException("Unable to get or create key " + e);
        }
    }

    private byte[] readMetadataBytes(String name) throws IOException, DataStoreException {
        DataRecord rec = getMetadataRecord(name);
        byte[] key = null;
        if (rec != null) {
            InputStream stream = null;
            try {
                stream = rec.getStream();
                return IOUtils.toByteArray(stream);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }
        return key;
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            BlockBlobClient blob = containerClient.getBlockBlobClient(key);
            BlobProperties properties = blob.getProperties();
            AzureBlobStoreDataRecord record = new AzureBlobStoreDataRecord(
                    this,
                    containerClient,
                    identifier,
                    blob.getProperties().lastModified().toEpochSecond(),
                    blob.getProperties().blobSize());
            LOG.debug("Data record read for blob. identifier={} duration={} record={}",
                      key, (System.currentTimeMillis() - start), record);
            return record;
        }
        catch (StorageException e) {
            LOG.info("Error getting data record for blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot retrieve blob. identifier=%s", key), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return new RecordsIterator<DataIdentifier>(
                new Function<AzureBlobInfo, DataIdentifier>() {
                    @Override
                    public DataIdentifier apply(AzureBlobInfo input) {
                        return new DataIdentifier(getIdentifierName(input.getName()));
                    }
                }
        );
    }



    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        final AbstractSharedBackend backend = this;
        return new RecordsIterator<DataRecord>(
                new Function<AzureBlobInfo, DataRecord>() {
                    @Override
                    public DataRecord apply(AzureBlobInfo input) {
                        return new AzureBlobStoreDataRecord(
                            backend,
                            containerClient,
                            new DataIdentifier(getIdentifierName(input.getName())),
                            input.getLastModified(),
                            input.getLength());
                    }
                }
        );
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean exists = containerClient.getBlockBlobClient(key).exists();
            LOG.debug("Blob exists={} identifier={} duration={}", exists, key, (System.currentTimeMillis() - start));
            return exists;
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
        LOG.info("AzureBlobBackend closed.");
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            VoidResponse rsp = containerClient.getBlockBlobClient(key).deleteWithResponse(null, null, null, Context.NONE);
            if (rsp.statusCode() < 400) {
                LOG.debug("Blob deleted. identifier={} duration={}", key,
                        (System.currentTimeMillis() - start));
            }
            else if (rsp.statusCode() == 404) {
                LOG.debug("Blob delete requested, but it does not exist (perhaps already deleted). identifier={} duration={}",
                        key,
                        (System.currentTimeMillis() - start));
            }
            else {
                LOG.warn("Blob delete failed - status={}. identifier={} duration={}", rsp.statusCode(), key,
                        (System.currentTimeMillis() - start));
            }
        }
        catch (StorageException e) {
            if (e.statusCode() == 404) {
                LOG.debug("Blob delete requested, but it does not exist (perhaps already deleted). identifier={} duration={}",
                        key,
                        (System.currentTimeMillis() - start));
            }
            else {
                LOG.info("Error deleting blob. identifier={}", key, e);
                throw new DataStoreException(e);
            }
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
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(input, name, -1L);
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
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
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(new FileInputStream(input), name, input.length());
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
        }
        catch (FileNotFoundException e) {
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
            String metaName = addMetaKeyPrefix(name);
            BlockBlobClient blob = containerClient.getBlockBlobClient(metaName);
            if (-1 == recordLength) {
                try (OutputStream blobStream = blob.getBlobOutputStream()) {
                    IOUtils.copy(input, blobStream);
                }
            }
            else {
                blob.upload(input, recordLength);
            }
        }
        catch (StorageException e) {
            LOG.info("Error adding metadata record. metadataName={} length={}", name, recordLength, e);
            throw new DataStoreException(e);
        }
        catch (IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            String metaName = addMetaKeyPrefix(name);
            BlockBlobClient blob = containerClient.getBlockBlobClient(metaName);
            if (!blob.exists()) {
                LOG.warn("Trying to read missing metadata. metadataName={}", name);
                return null;
            }
            BlobProperties properties = blob.getProperties();
            AzureBlobStoreDataRecord record =  new AzureBlobStoreDataRecord(this,
                    containerClient,
                    new DataIdentifier(name),
                    properties.lastModified().toEpochSecond(),
                    properties.blobSize(),
                    true);
            LOG.debug("Metadata record read. metadataName={} duration={} record={}", name, (System.currentTimeMillis() - start), record);
            return record;

        } catch (StorageException e) {
            LOG.info("Error reading metadata record. metadataName={}", name, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.debug("Error reading metadata record. metadataName={}", name, e);
            throw new RuntimeException(e);
        } finally {
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
        long start = System.currentTimeMillis();
        final List<DataRecord> records = Lists.newArrayList();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            String metaPrefix = addMetaKeyPrefix(prefix);
            final AzureBlobStoreBackend backend = this;
            containerClient.listBlobsFlat(new ListBlobsOptions().prefix(metaPrefix), null)
                    .forEach(
                            blobItem -> records.add(new AzureBlobStoreDataRecord(
                                    backend,
                                    containerClient,
                                    new DataIdentifier(stripMetaKeyPrefix(blobItem.name())),
                                    blobItem.properties().lastModified().toEpochSecond(),
                                    blobItem.properties().contentLength(),
                                    true
                            ))
                    );

            LOG.debug("Metadata records read. recordsRead={} metadataFolder={} duration={}", records.size(), prefix, (System.currentTimeMillis() - start));
        }
        catch (StorageException e) {
            LOG.info("Error reading all metadata records. metadataFolder={}", prefix, e);
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
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        boolean success = false;
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            BlockBlobClient blob = containerClient.getBlockBlobClient(addMetaKeyPrefix(name));
            VoidResponse rsp = blob.deleteWithResponse(null, null, null, Context.NONE);
            if (rsp.statusCode() < 400) {
                LOG.debug("Metadata record deleted. identifier={} duration={}", name,
                        (System.currentTimeMillis() - start));
                success = true;
            }
            else if (rsp.statusCode() == 404) {
                LOG.debug("Metadata record delete requested, but it does not exist (perhaps already deleted). identifier={} duration={}",
                        name,
                        (System.currentTimeMillis() - start));
                success = true;
            }
            else {
                LOG.warn("Metadata record delete failed - status={}. identifier={} duration={}", rsp.statusCode(), name,
                        (System.currentTimeMillis() - start));
            }

        }
        catch (StorageException e) {
            LOG.info("Error deleting metadata record. metadataName={}", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return success;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            String metaPrefix = addMetaKeyPrefix(prefix);
            final AtomicInteger total = new AtomicInteger(0);
            containerClient.listBlobsFlat(new ListBlobsOptions().prefix(metaPrefix), null)
                    .forEach(
                            blobItem -> {
                                if (400 > containerClient
                                        .getBlockBlobClient(blobItem.name())
                                        .deleteWithResponse(null, null, null, Context.NONE)
                                    .statusCode()) total.getAndIncrement();
                            }
                    );

            LOG.debug("Metadata records deleted. recordsDeleted={} metadataFolder={} duration={}",
                    total.get(), prefix, (System.currentTimeMillis() - start));

        }
        catch (StorageException e) {
            LOG.info("Error deleting all metadata records. metadataFolder={}", prefix, e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean metadataRecordExists(String name) {
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            BlockBlobClient blob = containerClient.getBlockBlobClient(addMetaKeyPrefix(name));
            boolean exists = blob.exists();
            LOG.debug("Metadata record {} exists {}. duration={}", name, exists, (System.currentTimeMillis() - start));
            return exists;
        }
        catch (StorageException e) {
            LOG.debug("Error checking existence of metadata record = {}", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
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

    private static String addMetaKeyPrefix(final String key) {
        return META_KEY_PREFIX + key;
    }

    private static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(META_KEY_PREFIX)) {
            return name.substring(META_KEY_PREFIX.length());
        }
        return name;
    }

    void setHttpDownloadURIExpirySeconds(int seconds) {
        httpDownloadURIExpirySeconds = seconds;
    }

    void setHttpDownloadURICacheSize(int maxSize) {
        // max size 0 or smaller is used to turn off the cache
        if (maxSize > 0) {
            LOG.info("presigned GET URI cache enabled, maxSize = {} items, expiry = {} seconds", maxSize, httpDownloadURIExpirySeconds / 2);
            httpDownloadURICache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(httpDownloadURIExpirySeconds / 2, TimeUnit.SECONDS)
                    .build();
        } else {
            LOG.info("presigned GET URI cache disabled");
            httpDownloadURICache = null;
        }
    }

    URI createHttpDownloadURI(@NotNull DataIdentifier identifier,
                              @NotNull DataRecordDownloadOptions downloadOptions) {
        URI uri = null;

        // When running unit test from Maven, it doesn't always honor the @NotNull decorators
        if (null == identifier) throw new NullPointerException("identifier");
        if (null == downloadOptions) throw new NullPointerException("downloadOptions");
        
        if (httpDownloadURIExpirySeconds > 0) {

            if (null != httpDownloadURICache) {
                uri = httpDownloadURICache.getIfPresent(identifier);
            }
            if (null == uri) {
                if (presignedDownloadURIVerifyExists) {
                    // Check if this identifier exists.  If not, we want to return null
                    // even if the identifier is in the download URI cache.
                    try {
                        if (!exists(identifier)) {
                            LOG.warn("Cannot create download URI for nonexistent blob {}; returning null", getKeyName(identifier));
                            return null;
                        }
                    } catch (DataStoreException e) {
                        LOG.warn("Cannot create download URI for blob {} (caught DataStoreException); returning null", getKeyName(identifier), e);
                        return null;
                    }
                }

                String key = getKeyName(identifier);
                BlockBlobClient blob = containerClient.getBlockBlobClient(key);
                BlobSASPermission permission = new BlobSASPermission().read(true);

                String cacheControl = String.format("private, max-age=%d, immutable", httpDownloadURIExpirySeconds);
                String contentType = downloadOptions.getContentTypeHeader();
                String contentDisposition = downloadOptions.getContentDispositionHeader();

                uri = createPresignedURI(key, blob, permission,
                        cacheControl, contentType, contentDisposition,
                        httpDownloadURIExpirySeconds);

//                SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
//                headers.setCacheControl(String.format("private, max-age=%d, immutable", httpDownloadURIExpirySeconds));
//
//                String contentType = downloadOptions.getContentTypeHeader();
//                if (! Strings.isNullOrEmpty(contentType)) {
//                    headers.setContentType(contentType);
//                }
//
//                String contentDisposition =
//                        downloadOptions.getContentDispositionHeader();
//                if (! Strings.isNullOrEmpty(contentDisposition)) {
//                    headers.setContentDisposition(contentDisposition);
//                }
//
//                uri = createPresignedURI(key,
//                        EnumSet.of(SharedAccessBlobPermissions.READ),
//                        httpDownloadURIExpirySeconds,
//                        headers);
                if (uri != null && httpDownloadURICache != null) {
                    httpDownloadURICache.put(identifier, uri);
                }
            }
        }
        return uri;
    }

    void setHttpUploadURIExpirySeconds(int seconds) { httpUploadURIExpirySeconds = seconds; }

    private DataIdentifier generateSafeRandomIdentifier() {
        return new DataIdentifier(
                String.format("%s-%d",
                        UUID.randomUUID().toString(),
                        Instant.now().toEpochMilli()
                )
        );
    }

    DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs) {
        List<URI> uploadPartURIs = Lists.newArrayList();
        long minPartSize = MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxPartSize = MAX_MULTIPART_UPLOAD_PART_SIZE;

        if (0L >= maxUploadSizeInBytes) {
            throw new IllegalArgumentException("maxUploadSizeInBytes must be > 0");
        }
        else if (0 == maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (-1 > maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (maxUploadSizeInBytes > MAX_SINGLE_PUT_UPLOAD_SIZE &&
                maxNumberOfURIs == 1) {
            throw new IllegalArgumentException(
                    String.format("Cannot do single-put upload with file size %d - exceeds max single-put upload size of %d",
                            maxUploadSizeInBytes,
                            MAX_SINGLE_PUT_UPLOAD_SIZE)
            );
        }
        else if (maxUploadSizeInBytes > MAX_BINARY_UPLOAD_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Cannot do upload with file size %d - exceeds max upload size of %d",
                            maxUploadSizeInBytes,
                            MAX_BINARY_UPLOAD_SIZE)
            );
        }

        DataIdentifier newIdentifier = generateSafeRandomIdentifier();
        String blobId = getKeyName(newIdentifier);
        String uploadId = null;

        if (httpUploadURIExpirySeconds > 0) {
            // Always do multi-part uploads for Azure, even for small binaries.
            //
            // This is because Azure requires a unique header, "x-ms-blob-type=BlockBlob", to be
            // set but only for single-put uploads, not multi-part.
            // This would require clients to know not only the type of service provider being used
            // but also the type of upload (single-put vs multi-part), which breaks abstraction.
            // Instead we can insist that clients always do multi-part uploads to Azure, even
            // if the multi-part upload consists of only one upload part.  This doesn't require
            // additional work on the part of the client since the "complete" request must always
            // be sent regardless, but it helps us avoid the client having to know what type
            // of provider is being used, or us having to instruct the client to use specific
            // types of headers, etc.

            // Azure doesn't use upload IDs like AWS does
            // Generate a fake one for compatibility - we use them to determine whether we are
            // doing multi-part or single-put upload
            uploadId = Base64.encode(UUID.randomUUID().toString());

            long numParts = 0L;
            if (maxNumberOfURIs > 0) {
                long requestedPartSize = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) maxNumberOfURIs));
                if (requestedPartSize <= maxPartSize) {
                    numParts = Math.min(
                            maxNumberOfURIs,
                            Math.min(
                                    (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) minPartSize)),
                                    MAX_ALLOWABLE_UPLOAD_URIS
                            )
                    );
                } else {
                    throw new IllegalArgumentException(
                            String.format("Cannot do multi-part upload with requested part size %d", requestedPartSize)
                    );
                }
            }
            else {
                long maximalNumParts = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) MIN_MULTIPART_UPLOAD_PART_SIZE));
                numParts = Math.min(maximalNumParts, MAX_ALLOWABLE_UPLOAD_URIS);
            }

            String key = getKeyName(newIdentifier);
            BlobSASPermission permission = new BlobSASPermission().write(true);
//            EnumSet<SharedAccessBlobPermissions> perms = EnumSet.of(SharedAccessBlobPermissions.WRITE);
            Map<String, String> presignedURIRequestParams = Maps.newHashMap();
            presignedURIRequestParams.put("comp", "block");
            for (long blockId = 1; blockId <= numParts; ++blockId) {
                presignedURIRequestParams.put("blockId",
                        Base64.encode(String.format("%06d", blockId)));
                uploadPartURIs.add(
                        createPresignedURI(key, containerClient.getBlockBlobClient(key),
                                permission, httpUploadURIExpirySeconds, presignedURIRequestParams));
            }
        }

        try {
            byte[] secret = getOrCreateReferenceKey();
            String uploadToken = new DataRecordUploadToken(blobId, uploadId).getEncodedToken(secret);
            return new DataRecordUpload() {
                @Override
                @NotNull
                public String getUploadToken() { return uploadToken; }

                @Override
                public long getMinPartSize() { return minPartSize; }

                @Override
                public long getMaxPartSize() { return maxPartSize; }

                @Override
                @NotNull
                public Collection<URI> getUploadURIs() { return uploadPartURIs; }
            };
        }
        catch (DataStoreException e) {
            LOG.warn("Unable to obtain data store key");
        }

        return null;
    }

    DataRecord completeHttpUpload(@NotNull String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException {

        if (Strings.isNullOrEmpty(uploadTokenStr)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        DataRecordUploadToken uploadToken = DataRecordUploadToken.fromEncodedToken(uploadTokenStr, getOrCreateReferenceKey());
        String key = uploadToken.getBlobId();
        DataIdentifier blobId = new DataIdentifier(getIdentifierName(key));

        DataRecord record = null;
        try {
            record = getRecord(blobId);
            // If this succeeds this means either it was a "single put" upload
            // (we don't need to do anything in this case - blob is already uploaded)
            // or it was completed before with the same token.
        }
        catch (DataStoreException e) {
            // record doesn't exist - so this means we are safe to do the complete request
            try {
                if (uploadToken.getUploadId().isPresent()) {
                    BlockBlobClient blob = containerClient.getBlockBlobClient(key);
                    BlockList blockList = blob.listBlocks(BlockListType.UNCOMMITTED);
                    List<String> blockIds = Lists.newArrayList();
                    long size = 0L;
                    for (Block block : blockList.uncommittedBlocks()) {
                        blockIds.add(block.name());
                        size += block.size();
                    }
                    blob.commitBlockList(blockIds);

                    record = new AzureBlobStoreDataRecord(
                            this,
                            containerClient,
                            blobId,
                            blob.getProperties().lastModified().toEpochSecond(),
                            size);
                }
                else {
                    // Something is wrong - upload ID missing from upload token
                    // but record doesn't exist already, so this is invalid
                    throw new DataRecordUploadException(
                            String.format("Unable to finalize direct write of binary %s - upload ID missing from upload token",
                                    blobId)
                    );
                }
            } catch (StorageException e2) {
                throw new DataRecordUploadException(
                        String.format("Unable to finalize direct write of binary %s", blobId),
                        e
                );
            }
        }

        return record;
    }

    private URI createPresignedURI(String key,
                                   BlockBlobClient blobClient,
                                   BlobSASPermission permission,
                                   String cacheControlHeader,
                                   String contentTypeHeader,
                                   String contentDispositionHeader,
                                   int expirySeconds) {
        return createPresignedURI(key, blobClient, permission, cacheControlHeader, contentTypeHeader, contentDispositionHeader, expirySeconds, Maps.newHashMap());
    }

    private URI createPresignedURI(String key,
                                   BlockBlobClient blobClient,
                                   BlobSASPermission permission,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams) {
        return createPresignedURI(key, blobClient, permission, null, null, null, expirySeconds, additionalQueryParams);
    }

    private URI createPresignedURI(String key,
                                   BlockBlobClient blobClient,
                                   BlobSASPermission permission,
                                   String cacheControlHeader,
                                   String contentTypeHeader,
                                   String contentDispositionHeader,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams) {
        OffsetDateTime start = OffsetDateTime.now().minusMinutes(15); // Account for potential time drift between server and cloud service
        OffsetDateTime expiry = OffsetDateTime.now().plusSeconds(expirySeconds);

        try {
            // URI class will re-encode this signature
            String signature = blobClient.generateSAS(
                    null,
                    permission,
                    expiry,
                    start,
                    null,
                    null,
                    null,
                    Strings.isNullOrEmpty(cacheControlHeader) ? null : cacheControlHeader,
                    Strings.isNullOrEmpty(contentDispositionHeader) ? null : contentDispositionHeader,
                    null,
                    null,
                    Strings.isNullOrEmpty(contentTypeHeader) ? null : contentTypeHeader
            );

            for (Map.Entry<String, String> p : additionalQueryParams.entrySet()) {
                signature = signature + "&" + URLEncoder.encode(p.getKey(), "UTF-8")
                        + "=" + URLEncoder.encode(p.getValue(), "UTF-8");
            }

            URL blobUrl = blobClient.getBlobUrl();
            URI uri = new URI(String.format("%s://%s%s?%s",
                    blobUrl.getProtocol(),
                    blobUrl.getHost(),
                    blobUrl.getPath(),
                    signature));
            return uri;
        }
        catch (URISyntaxException | UnsupportedEncodingException e) {
            LOG.error("Can't generate a presigned URI for key {}", key, e);
        }
        catch (StorageException e) {
            LOG.error("Azure request to create presigned Azure Blob Storage {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, Azure Error Code: {}",
                    permission.read() ? "GET" :
                            (permission.write() ? "PUT" : ""),
                    key,
                    e.getMessage(),
                    e.statusCode(),
                    e.errorCode());
        }

        return null;
    }

    private static class AzureBlobInfo {
        private final String name;
        private final long lastModified;
        private final long length;

        public AzureBlobInfo(String name, long lastModified, long length) {
            this.name = name;
            this.lastModified = lastModified;
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public long getLastModified() {
            return lastModified;
        }

        public long getLength() {
            return length;
        }

//        public static AzureBlobInfo fromCloudBlob(CloudBlob cloudBlob) {
//            return new AzureBlobInfo(cloudBlob.getName(),
//                                     cloudBlob.getProperties().getLastModified().getTime(),
//                                     cloudBlob.getProperties().getLength());
//        }

        public static AzureBlobInfo fromBlobItem(BlobItem blobItem) {
            return new AzureBlobInfo(blobItem.name(),
                    blobItem.properties().lastModified().toEpochSecond(),
                    blobItem.properties().contentLength());
        }
    }

    private class RecordsIterator<T> extends AbstractIterator<T> {
        // Seems to be thread-safe (in 5.0.0)
//        ResultContinuation resultContinuation;
        String resultContinuation;
        boolean firstCall = true;
        final Function<AzureBlobInfo, T> transformer;
        final Queue<AzureBlobInfo> items = Lists.newLinkedList();

        public RecordsIterator (Function<AzureBlobInfo, T> transformer) {
            this.transformer = transformer;
        }

        @Override
        protected T computeNext() {
            if (items.isEmpty()) {
                loadItems();
            }
            if (!items.isEmpty()) {
                return transformer.apply(items.remove());
            }
            return endOfData();
        }

        private boolean loadItems() {
            long start = System.currentTimeMillis();
            ClassLoader contextClassLoader = currentThread().getContextClassLoader();
            try {
                currentThread().setContextClassLoader(getClass().getClassLoader());

                if (!firstCall && (null == resultContinuation)) {
                    LOG.trace("No more records in container. containerName={}", containerName);
                    return false;
                }
                firstCall = false;

                AtomicInteger nResults = new AtomicInteger(0);
                containerClient.listBlobsFlat()
                        .streamByPage(resultContinuation)
                        .forEach(result -> {
                            nResults.addAndGet(result.value().size());
                            items.addAll(Lists.transform(
                                    result.value(),
                                    blobItem -> AzureBlobInfo.fromBlobItem(blobItem)
                            ));
                            resultContinuation = result.nextLink();
                        });

                LOG.debug("Container records batch read. batchSize={} containerName={} duration={}",
                        nResults.get(), containerName,  (System.currentTimeMillis() - start));
                return nResults.get() > 0;


//                CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
//                if (!firstCall && (resultContinuation == null || !resultContinuation.hasContinuation())) {
//                    LOG.trace("No more records in container. containerName={}", container);
//                    return false;
//                }
//                firstCall = false;
//
//                ResultSegment<ListBlobItem> results = container.listBlobsSegmented(null, false, EnumSet.noneOf(BlobListingDetails.class), null, resultContinuation, null, null);
//                resultContinuation = results.getContinuationToken();
//                for (ListBlobItem item : results.getResults()) {
//                    if (item instanceof CloudBlob) {
//                        items.add(AzureBlobInfo.fromCloudBlob((CloudBlob)item));
//                    }
//                }
//
//                LOG.debug("Container records batch read. batchSize={} containerName={} duration={}",
//                        results.getLength(), containerName,  (System.currentTimeMillis() - start));
//                return results.getLength() > 0;
            }
            catch (StorageException e) {
                LOG.info("Error listing blobs. containerName={}", containerName, e);
//            }
//            catch (DataStoreException e) {
//                LOG.debug("Cannot list blobs. containerName={}", containerName, e);
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
            return false;
        }
    }

    static class AzureBlobStoreDataRecord extends AbstractDataRecord {
        final ContainerClient client;
        final long lastModified;
        final long length;
        final boolean isMeta;

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, ContainerClient client,
                                        DataIdentifier key, long lastModified, long length) {
            this(backend, client, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, ContainerClient client,
                                        DataIdentifier key, long lastModified, long length, boolean isMeta) {
            super(backend, key);
            this.client = client;
            this.lastModified = lastModified;
            this.length = length;
            this.isMeta = isMeta;
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            String id = getKeyName(getIdentifier());
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
            }
            else {
                // Don't worry about stream logging for metadata records
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={} ", id, new Exception());
                }
            }
            try {
                return client.getBlockBlobClient(id).openInputStream();
            } catch (StorageException e) {
                throw new DataStoreException(e);
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }

        @Override
        public String toString() {
            return "AzureBlobStoreDataRecord{" +
                   "identifier=" + getIdentifier() +
                   ", length=" + length +
                   ", lastModified=" + lastModified +
                   '}';
        }
    }
}
