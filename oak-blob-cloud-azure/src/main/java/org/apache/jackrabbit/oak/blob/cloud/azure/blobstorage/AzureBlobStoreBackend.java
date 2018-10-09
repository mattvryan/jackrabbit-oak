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
import static org.apache.jackrabbit.oak.blob.cloud.Constants.META_DIR_NAME;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.RequestOptions;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.BlockEntry;
import com.microsoft.azure.storage.blob.BlockListingFilter;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CopyStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.SharedAccessBlobHeaders;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.AbstractCloudBackend;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobStoreBackend extends AbstractCloudBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);

    private static final long BUFFERED_STREAM_THRESHHOLD = 1024 * 1024;
    static final long MIN_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 10; // 10MB
    static final long MAX_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 100; // 100MB
    static final long MAX_SINGLE_PUT_UPLOAD_SIZE = 1024 * 1024 * 256; // 256MB, Azure limit
    static final long MAX_BINARY_UPLOAD_SIZE = (long) Math.floor(1024L * 1024L * 1024L * 1024L * 4.75); // 4.75TB, Azure limit
    private static final int MAX_ALLOWABLE_UPLOAD_URIS = 50000; // Azure limit
    private static final int MAX_UNIQUE_RECORD_TRIES = 10;

    private String containerName;
    private String connectionString;
    private int concurrentRequestCount = 1;
    private RetryPolicy retryPolicy;
    private Integer requestTimeout;

    protected CloudBlobContainer getAzureContainer() throws DataStoreException {
        CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
        RequestOptions requestOptions = container.getServiceClient().getDefaultRequestOptions();
        if (retryPolicy != null) {
            requestOptions.setRetryPolicyFactory(retryPolicy);
        }
        if (requestTimeout != null) {
            requestOptions.setTimeoutIntervalInMs(requestTimeout);
        }
        return container;
    }

    @Override
    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            LOG.debug("Started backend initialization");

            Properties properties = getProperties();
            if (null == properties) {
                try {
                    properties = Utils.readConfig(Utils.DEFAULT_CONFIG_FILE);
                    setProperties(properties);
                }
                catch (IOException e) {
                    throw new DataStoreException("Unable to initialize Azure Data Store from " + Utils.DEFAULT_CONFIG_FILE, e);
                }
            }

            try {
                Utils.setProxyIfNeeded(properties);
                containerName = (String) properties.get(AzureConstants.AZURE_BLOB_CONTAINER_NAME);
                connectionString = Utils.getConnectionStringFromProperties(properties);
                concurrentRequestCount = PropertiesUtil.toInteger(properties.get(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION), 1);
                LOG.info("Using concurrentRequestsPerOperation={}", concurrentRequestCount);
                retryPolicy = Utils.getRetryPolicy((String)properties.get(AzureConstants.AZURE_BLOB_MAX_REQUEST_RETRY));
                if (properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT) != null) {
                    requestTimeout = PropertiesUtil.toInteger(properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT), RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
                }

                CloudBlobContainer azureContainer = getAzureContainer();

                if (azureContainer.createIfNotExists()) {
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
            catch (StorageException e) {
                throw new DataStoreException(e);
            }
        }
        finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Nullable
    @Override
    protected InputStream readObject(@NotNull final String key) throws DataStoreException {
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (!blob.exists()) {
                return null;
            }
            return blob.openInputStream();
        }
        catch (StorageException | URISyntaxException e) {
            String id = getIdentifierName(key);
            LOG.info("Error reading blob [{}]", id, e);
            throw new DataStoreException(
                    String.format("Error reading blob [%s]", id),
                    e
            );
        }
    }

    @Nullable
    @Override
    protected BlobAttributes getBlobAttributes(@NotNull final String key) throws DataStoreException {
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (blob.exists()) {
                blob.downloadAttributes();
                return new BlobAttributes() {
                    @Override
                    public long getLength() {
                        return blob.getProperties().getLength();
                    }

                    @Override
                    public long getLastModifiedTime() {
                        return blob.getProperties().getLastModified().getTime();
                    }
                };
            }
            return null;
        }
        catch (StorageException | URISyntaxException e) {
            throw new DataStoreException(
                    String.format("Error getting blob attributes for blob [%s]",
                            getIdentifierName(key)),
                    e
            );
        }
    }

    @Override
    protected void touchObject(@NotNull final String key) throws DataStoreException {
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            blob.startCopy(blob);
            //TODO: better way of updating lastModified (use custom metadata?)
            if (!waitForCopy(blob)) {
                throw new DataStoreException(
                        String.format("Error updating lastModified time for blob [%s], status=%s",
                                getIdentifierName(key), blob.getCopyState().getStatusDescription())
                );
            }
        }
        catch (StorageException | URISyntaxException | InterruptedException e) {
            throw new DataStoreException(
                    String.format("Error updating lastModified time for blob [%s]",
                            getIdentifierName(key)),
                    e
            );
        }
    }

    @Override
    protected void writeObject(@NotNull final String key,
                               @NotNull final InputStream in,
                               long length) throws DataStoreException {
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            BlobRequestOptions options = new BlobRequestOptions();
            options.setConcurrentRequestCount(concurrentRequestCount);
            boolean useBufferedStream = length < BUFFERED_STREAM_THRESHHOLD;
            if (useBufferedStream) {
                try (InputStream bufferedStream = new BufferedInputStream(in)) {
                    blob.upload(bufferedStream, length, null, options, null);
                }
            } else {
                blob.upload(in, length, null, options, null);
            }
        }
        catch (StorageException | URISyntaxException | IOException e) {
            throw new DataStoreException(
                    String.format("Error writing blob [%s]",
                            getIdentifierName(key)),
                    e
            );
        }
    }

    private static boolean waitForCopy(CloudBlob blob) throws StorageException, InterruptedException {
        boolean continueLoop = true;
        CopyStatus status = CopyStatus.PENDING;
        while (continueLoop) {
            blob.downloadAttributes();
            status = blob.getCopyState().getStatus();
            continueLoop = status == CopyStatus.PENDING;
            // Sleep if retry is needed
            if (continueLoop) {
                Thread.sleep(500);
            }
        }
        return status == CopyStatus.SUCCESS;
    }

    @Nullable
    @Override
    protected DataRecord getObjectDataRecord(@NotNull final DataIdentifier identifier)
            throws DataStoreException {
        return getDataRecordImpl(identifier, getKeyName(identifier), false);
    }

    @Nullable
    @Override
    protected DataRecord getObjectMetadataRecord(@NotNull final String name)
            throws DataStoreException {
        return getDataRecordImpl(new DataIdentifier(name), addMetaKeyPrefix(name), true);
    }

    @Nullable
    private DataRecord getDataRecordImpl(@NotNull final DataIdentifier identifier,
                                         @NotNull final String key,
                                         boolean isMetadataRecord)
            throws DataStoreException {
        DataRecord record = null;
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            if (blob.exists()) {
                blob.downloadAttributes();
                record = new AzureBlobStoreDataRecord(
                        this,
                        connectionString,
                        containerName,
                        identifier,
                        blob.getProperties().getLastModified().getTime(),
                        blob.getProperties().getLength(),
                        isMetadataRecord);
            }
        }
        catch (StorageException | URISyntaxException e) {
            LOG.info("Error getting data record for blob. identifier=[{}]", identifier, e);
            throw new DataStoreException(
                    String.format("Error getting data record for blob. identifier=[%s]",
                            identifier),
                    e
            );
        }
        return record;
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
                            connectionString,
                            containerName,
                            new DataIdentifier(getIdentifierName(input.getName())),
                            input.getLastModified(),
                            input.getLength());
                    }
                }
        );
    }

    @Override
    protected boolean objectExists(@NotNull final String key) throws DataStoreException {
        try {
            return getAzureContainer().getBlockBlobReference(key).exists();
        }
        catch (StorageException | URISyntaxException e) {
            throw new DataStoreException(
                    String.format("Error occurred checking object existence for key [%s]",
                            key),
                    e);
        }
    }

    @Override
    public void close() throws DataStoreException {
        LOG.info("AzureBlobBackend closed.");
    }

    @Override
    protected boolean deleteObject(@NotNull final String key) throws DataStoreException {
        try {
            return getAzureContainer().getBlockBlobReference(key).deleteIfExists();
        }
        catch (StorageException | URISyntaxException e) {
            LOG.info("Error deleting blob. identifier={}", getIdentifierName(key), e);
            throw new DataStoreException(e);
        }
    }

    @NotNull
    @Override
    protected List<DataRecord> getAllObjectMetadataRecords(@NotNull final String prefix) {
        final List<DataRecord> records = Lists.newArrayList();
        try {
            CloudBlobDirectory metaDir = getAzureContainer().getDirectoryReference(META_DIR_NAME);
            for (ListBlobItem item : metaDir.listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    CloudBlob blob = (CloudBlob) item;
                    records.add(new AzureBlobStoreDataRecord(
                            this,
                            connectionString,
                            containerName,
                            new DataIdentifier(stripMetaKeyPrefix(blob.getName())),
                            blob.getProperties().getLastModified().getTime(),
                            blob.getProperties().getLength(),
                            true));
                }
            }
        }
        catch (StorageException e) {
            LOG.info("Error reading all metadata records. metadataFolder={}", prefix, e);
        }
        catch (DataStoreException | URISyntaxException e) {
            LOG.debug("Error reading all metadata records. metadataFolder={}", prefix, e);
        }
        return records;
    }

    @Override
    protected int deleteAllObjectMetadataRecords(@NotNull final String prefix) {
        int total = 0;
        try {
            for (ListBlobItem item : getAzureContainer().listBlobs(prefix)) {
                if (item instanceof CloudBlob) {
                    if (((CloudBlob)item).deleteIfExists()) {
                        total++;
                    }
                }
            }
        }
        catch (StorageException | DataStoreException e) {
            LOG.info("Error deleting all metadata records. metadataFolder={}", prefix, e);
        }
        return total;
    }


    // Direct Binary Access

    public void setBinaryTransferAccelerationEnabled(boolean enabled) {
        // No-op - not supported in Azure Blob Storage
    }

    @Nullable
    @Override
    protected URI createPresignedGetURI(@NotNull final DataIdentifier identifier,
                                        @NotNull final DataRecordDownloadOptions downloadOptions) {
        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setCacheControl(String.format("private, max-age=%d, immutable", getHttpDownloadURIExpirySeconds()));

        String contentType = downloadOptions.getContentTypeHeader();
        if (! Strings.isNullOrEmpty(contentType)) {
            headers.setContentType(contentType);
        }

        String contentDisposition =
                downloadOptions.getContentDispositionHeader();
        if (! Strings.isNullOrEmpty(contentDisposition)) {
            headers.setContentDisposition(contentDisposition);
        }

        return createPresignedURI(identifier,
                EnumSet.of(SharedAccessBlobPermissions.READ),
                getHttpDownloadURIExpirySeconds(),
                Maps.newHashMap(),
                headers);
    }

    private URI createPresignedPutURI(@NotNull final DataIdentifier identifier,
                                      @NotNull final Map<String, String> additionalQueryParams) {
        return createPresignedURI(identifier,
                EnumSet.of(SharedAccessBlobPermissions.WRITE),
                getHttpUploadURIExpirySeconds(),
                additionalQueryParams,
                null);
    }

    private URI createPresignedURI(@NotNull final DataIdentifier identifier,
                                   @NotNull final EnumSet<SharedAccessBlobPermissions> permissions,
                                   int expirySeconds,
                                   @NotNull final Map<String, String> additionalQueryParams,
                                   @Nullable final SharedAccessBlobHeaders optionalHeaders) {
        String key = getKeyName(identifier);

        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        Date expiry = Date.from(Instant.now().plusSeconds(expirySeconds));
        policy.setSharedAccessExpiryTime(expiry);
        policy.setPermissions(permissions);

        Properties properties = getProperties();
        String accountName = properties != null
                ? properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "")
                : null;
        if (Strings.isNullOrEmpty(accountName)) {
            LOG.warn("Can't generate presigned URI - Azure account name not found in properties");
            return null;
        }

        URI presignedURI = null;
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            String sharedAccessSignature =
                    null == optionalHeaders ?
                            blob.generateSharedAccessSignature(policy,
                                    null) :
                            blob.generateSharedAccessSignature(policy,
                                    optionalHeaders,
                                    null);
            // Shared access signature is returned encoded already.

            String uriString = String.format("https://%s.blob.core.windows.net/%s/%s?%s",
                    accountName,
                    containerName,
                    key,
                    sharedAccessSignature);

            if (! additionalQueryParams.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<String, String> e : additionalQueryParams.entrySet()) {
                    builder.append("&");
                    builder.append(URLEncoder.encode(e.getKey(), Charsets.UTF_8.name()));
                    builder.append("=");
                    builder.append(URLEncoder.encode(e.getValue(), Charsets.UTF_8.name()));
                }
                uriString += builder.toString();
            }

            presignedURI = new URI(uriString);
        }
        catch (DataStoreException e) {
            LOG.error("No connection to Azure Blob Storage", e);
        }
        catch (URISyntaxException | InvalidKeyException | UnsupportedEncodingException e) {
            LOG.error("Can't generate a presigned URI for key {}", key, e);
        }
        catch (StorageException e) {
            LOG.error("Azure request to create presigned Azure Blob Storage {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, Azure Error Code: {}",
                    permissions.contains(SharedAccessBlobPermissions.READ) ? "GET" :
                            (permissions.contains(SharedAccessBlobPermissions.WRITE) ? "PUT" : ""),
                    key,
                    e.getMessage(),
                    e.getHttpStatusCode(),
                    e.getErrorCode());
        }

        return presignedURI;
    }

    @Nullable
    @Override
    public DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs) {
        verifyDirectUpload(maxUploadSizeInBytes,
                maxNumberOfURIs,
                MAX_SINGLE_PUT_UPLOAD_SIZE,
                MAX_BINARY_UPLOAD_SIZE);

        List<URI> uploadPartURIs = Lists.newArrayList();
        DataIdentifier newIdentifier = generateSafeRandomIdentifier();
        String uploadId = null;

        if (getHttpUploadURIExpirySeconds() > 0) {
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

            long numParts = getNumUploadParts(maxUploadSizeInBytes,
                    maxNumberOfURIs,
                    MAX_ALLOWABLE_UPLOAD_URIS,
                    MIN_MULTIPART_UPLOAD_PART_SIZE,
                    MAX_MULTIPART_UPLOAD_PART_SIZE);

            Map<String, String> presignedURIRequestParams = Maps.newHashMap();
            presignedURIRequestParams.put("comp", "block");
            for (long blockId = 1; blockId <= numParts; ++blockId) {
                presignedURIRequestParams.put("blockId",
                        Base64.encode(String.format("%06d", blockId)));
                uploadPartURIs.add(createPresignedPutURI(newIdentifier, presignedURIRequestParams));
            }
        }

        return createDataRecordUpload(getKeyName(newIdentifier),
                uploadId,
                MIN_MULTIPART_UPLOAD_PART_SIZE,
                MAX_MULTIPART_UPLOAD_PART_SIZE,
                uploadPartURIs);
    }

    @Override
    protected void completeMultiPartUpload(@NotNull final DataRecordUploadToken uploadToken, @NotNull final String key)
            throws DataRecordUploadException, DataStoreException {
        try {
            CloudBlockBlob blob = getAzureContainer().getBlockBlobReference(key);
            List<BlockEntry> blocks = blob.downloadBlockList(
                    BlockListingFilter.UNCOMMITTED,
                    AccessCondition.generateEmptyCondition(),
                    null,
                    null);
            blob.commitBlockList(blocks);
        }
        catch (URISyntaxException | StorageException e) {
            throw new DataRecordUploadException(
                    String.format("Unable to finalize direct write of binary %s", key));
        }
    }

    @Override
    protected void completeSinglePutUpload(@NotNull final DataRecordUploadToken uploadToken, @NotNull final String key)
            throws DataRecordUploadException, DataStoreException {
        // No-op - nothing to do for single-put uploads on azure
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

        public static AzureBlobInfo fromCloudBlob(CloudBlob cloudBlob) {
            return new AzureBlobInfo(cloudBlob.getName(),
                                     cloudBlob.getProperties().getLastModified().getTime(),
                                     cloudBlob.getProperties().getLength());
        }
    }

    private class RecordsIterator<T> extends AbstractIterator<T> {
        // Seems to be thread-safe (in 5.0.0)
        ResultContinuation resultContinuation;
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

                CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
                if (!firstCall && (resultContinuation == null || !resultContinuation.hasContinuation())) {
                    LOG.trace("No more records in container. containerName={}", container);
                    return false;
                }
                firstCall = false;
                ResultSegment<ListBlobItem> results = container.listBlobsSegmented(null, false, EnumSet.noneOf(BlobListingDetails.class), null, resultContinuation, null, null);
                resultContinuation = results.getContinuationToken();
                for (ListBlobItem item : results.getResults()) {
                    if (item instanceof CloudBlob) {
                        items.add(AzureBlobInfo.fromCloudBlob((CloudBlob)item));
                    }
                }
                LOG.debug("Container records batch read. batchSize={} containerName={} duration={}",
                          results.getLength(), containerName,  (System.currentTimeMillis() - start));
                return results.getLength() > 0;
            }
            catch (StorageException e) {
                LOG.info("Error listing blobs. containerName={}", containerName, e);
            }
            catch (DataStoreException e) {
                LOG.debug("Cannot list blobs. containerName={}", containerName, e);
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
            return false;
        }
    }

    static class AzureBlobStoreDataRecord extends AbstractDataRecord {
        final String connectionString;
        final String containerName;
        final long lastModified;
        final long length;
        final boolean isMeta;

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, String connectionString, String containerName,
                                        DataIdentifier key, long lastModified, long length) {
            this(backend, connectionString, containerName, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, String connectionString, String containerName,
                                        DataIdentifier key, long lastModified, long length, boolean isMeta) {
            super(backend, key);
            this.connectionString = connectionString;
            this.containerName = containerName;
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
            CloudBlobContainer container = Utils.getBlobContainer(connectionString, containerName);
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
            }
            if (LOG.isDebugEnabled()) {
                // Log message, with exception so we can get a trace to see where the call
                // came from
                LOG.debug("binary downloaded from Azure Blob Storage: " + getIdentifier(),
                        new Exception());
            }
            try {
                return container.getBlockBlobReference(id).openInputStream();
            } catch (StorageException | URISyntaxException e) {
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
                   ", containerName='" + containerName + '\'' +
                   '}';
        }
    }
}
