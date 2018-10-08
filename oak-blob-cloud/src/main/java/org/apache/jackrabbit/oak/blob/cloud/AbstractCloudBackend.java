/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.blob.cloud;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCloudBackend extends AbstractSharedBackend implements CloudBackend {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCloudBackend.class);

    protected static final String REF_KEY = "reference.key";

    abstract protected boolean objectExists(@NotNull final String key) throws DataStoreException;
    abstract protected @Nullable InputStream readObject(@NotNull final String key) throws DataStoreException;
    abstract protected @Nullable BlobAttributes getBlobAttributes(@NotNull final String key) throws DataStoreException;
    abstract protected void touchObject(@NotNull final String key) throws DataStoreException;
    abstract protected void writeObject(@NotNull final String key, @NotNull final InputStream in, long length) throws DataStoreException;
    abstract protected boolean deleteObject(@NotNull final String key) throws DataStoreException;
    abstract protected @Nullable DataRecord getObjectDataRecord(@NotNull final DataIdentifier identifier) throws DataStoreException;
    abstract protected @Nullable DataRecord getObjectMetadataRecord(@NotNull final String name) throws DataStoreException;
    abstract protected @NotNull List<DataRecord> getAllObjectMetadataRecords(@NotNull final String prefix);
    abstract protected int deleteAllObjectMetadataRecords(@NotNull final String prefix);
    abstract protected void completeMultiPartUpload(@NotNull final DataRecordUploadToken uploadToken, @NotNull final String key) throws DataRecordUploadException, DataStoreException;
    abstract protected void completeSinglePutUpload(@NotNull final DataRecordUploadToken uploadToken, @NotNull final String key) throws DataRecordUploadException, DataStoreException;
    abstract protected @Nullable URI createPresignedGetURI(@NotNull final DataIdentifier identifier, @NotNull final DataRecordDownloadOptions downloadOptions);

    private Properties properties;
    private byte[] secret;
    private Cache<DataIdentifier, URI> httpDownloadURICache;

    // 0 = disabled (default)
    private int httpDownloadURIExpirySeconds = 0;
    private int httpUploadURIExpirySeconds = 0;

    @Override
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    protected Properties getProperties() {
        return properties;
    }

    /**
     * Get key from data identifier. Object is stored with key in cloud storage.
     */
    protected static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + Utils.DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    protected static String getIdentifierName(String key) {
        if (!key.contains(Utils.DASH)) {
            return null;
        } else if (key.contains(Utils.META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }

    /**
     * Checks to see if blob identified by {@code identifier} exists in the
     * cloud storage.
     * @param identifier
     *            identifier of the blob to be checked.
     * @return true if the identifier matches an existing blob; false otherwise.
     * @throws DataStoreException if the attempt to check for the blob fails.
     */
    @Override
    public boolean exists(@NotNull final DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            boolean result = objectExists(key);
            if (result) {
                LOG.trace("Blob [{}] exists=true duration={}",
                        identifier,
                        System.currentTimeMillis()-start);
            }
            else {
                LOG.debug("Blob [{}] exists=false duration={}",
                        identifier,
                        System.currentTimeMillis()-start);
            }
            return result;
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Reads a blob identified by {@code identifier} from cloud storage.
     *
     * The blob is returned as an input stream.  The caller of this function
     * should consume the stream as soon as possible after calling this function
     * and then close the stream.  It is the responsibility of the caller to
     * close the input stream that is returned.
     *
     * @param identifier
     *            identifier (name) of blob to be read.
     * @return InputStream of the blob.
     * @throws DataStoreException if the blob cannot be read.
     */
    @NotNull
    @Override
    public InputStream read(@NotNull final DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            InputStream result = readObject(key);
            if (null == result) {
                throw new DataStoreException(
                        String.format("Tried to read blob [%s] but blob does not exist",
                                identifier)
                );
            }
            LOG.debug("[{}] read took [{}]ms", identifier, (System.currentTimeMillis() - start));
            if (LOG.isDebugEnabled()) {
                // Log message to help in tracing uses of read
                // Helpful if a client wants to locate uses to switch to direct binary access
                LOG.debug("Binary [{}] downloaded from cloud storage: ", identifier, new Exception());
            }
            return result;
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Uploads a file to cloud storage.
     * @param identifier
     *            identifier to be used as the name of the file in cloud storage
     * @param file
     *            file that will be stored in the cloud storage.
     * @throws DataStoreException if the attempt to store the file fails.
     */
    @Override
    public void write(@NotNull final DataIdentifier identifier,
                      @NotNull final File file) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            long newObjectLength = file.length();
            BlobAttributes attributes = getBlobAttributes(key);
            if (null != attributes) {
                if (newObjectLength != attributes.getLength()) {
                    throw new DataStoreException(
                            String.format("Error writing blob [%s]: Collision detected - new length: [%d], old length [%d]",
                                    identifier, newObjectLength, attributes.getLength())
                    );
                }
                LOG.debug("Blob [{}] exists, lastmodified=[{}]",
                        identifier, attributes.getLastModifiedTime());
                touchObject(key);
                LOG.debug("Blob [{}] timestamp updated, lastModified=[{}] duration=[{}]",
                        identifier,
                        attributes.getLastModifiedTime(),
                        (System.currentTimeMillis() - start));
            }
            else {
                try (InputStream in = new FileInputStream(file)) {
                    writeObject(key, in, newObjectLength);
                    LOG.debug("Blob [{}] created, length=[{}] duration=[{}]",
                            key, newObjectLength, (System.currentTimeMillis() - start));
                }
                catch (IOException e) {
                    throw new DataStoreException(
                            String.format("Unable to open input stream for blob [%s]",
                                    identifier),
                            e
                    );
                }
            }
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @NotNull
    @Override
    public DataRecord getRecord(@NotNull final DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            DataRecord record = getObjectDataRecord(identifier);
            if (null != record) {
                LOG.debug("Identifier [{}]'s getRecord = [{}] took [{}]ms.",
                        identifier, record, (System.currentTimeMillis() - start));
                return record;
            }
            else {
                LOG.info("getRecord for identifier [{}] - not found.  Took [{}]ms.",
                        identifier, (System.currentTimeMillis() - start));
                throw new DataStoreException(
                        String.format("No record found for identifier [%s]", identifier)
                );
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void deleteRecord(@NotNull final DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
            boolean result = deleteObject(key);
            LOG.debug("Blob {}. identifier={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    getIdentifierName(key),
                    (System.currentTimeMillis() - start)
            );
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(@NotNull final InputStream inputStream,
                                  @NotNull final String name) throws DataStoreException {
        addMetadataRecordImpl(inputStream, name, -1L);
    }

    @Override
    public void addMetadataRecord(@NotNull final File file,
                                  @NotNull final String name) throws DataStoreException {
        try {
            addMetadataRecordImpl(new FileInputStream(file), name, file.length());
        }
        catch (IOException e) {
            throw new DataStoreException(
                    String.format("Error writing metadata record for file %s",
                            file.getAbsolutePath()),
                    e
            );
        }
    }

    private void addMetadataRecordImpl(@NotNull final InputStream inputStream,
                                       @NotNull final String name,
                                       long length) throws DataStoreException {
        // Command-line maven doesn't seem to honor the @NotNull annotations
        if (null == inputStream) {
            throw new IllegalArgumentException("Input must not be null");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name cannot be empty");
        }

        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            writeObject(addMetaKeyPrefix(name), inputStream, length);
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Nullable
    @Override
    public DataRecord getMetadataRecord(@NotNull final String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        DataRecord record = null;
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            record = getObjectMetadataRecord(name);
            if (null == record) {
                LOG.info("Metadata record not found. metadataName={} duration ={} record={}",
                        name, (System.currentTimeMillis() - start), record);
            }
            else {
                LOG.debug("Metadata record read. metadataName={} duration={} record={}",
                        name, (System.currentTimeMillis() - start), record);
            }
        }
        catch (DataStoreException e) {
            LOG.error("Error reading metadata record. metadataName={}", name, e);
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return record;
    }

    @NotNull
    @Override
    public List<DataRecord> getAllMetadataRecords(@NotNull final String prefix) {
        // Command-line maven doesn't seem to honor the @NotNull annotations
        if (null == prefix) {
            throw new IllegalArgumentException("prefix must not be null");
        }

        long start = System.currentTimeMillis();
        List<DataRecord> records = Lists.newArrayList();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            records = getAllObjectMetadataRecords(prefix);
            LOG.debug("Metadata records read. recordsRead={} metadataFolder={} duration={}", records.size(), prefix, (System.currentTimeMillis() - start));
            return records;
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean deleteMetadataRecord(@NotNull final String name) {
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            boolean result = deleteObject(addMetaKeyPrefix(name));
            LOG.debug("Metadata record {}. metadataName={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    name, (System.currentTimeMillis() - start));
            return result;
        }
        catch (DataStoreException e) {
            LOG.debug("Error deleting metadata record. metadataName={}", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(@NotNull final String prefix) {
        // Command-line maven doesn't seem to honor the @NotNull annotations
        if (null == prefix) {
            throw new IllegalArgumentException("prefix must not be null");
        }

        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            int total = deleteAllObjectMetadataRecords(addMetaKeyPrefix(prefix));
            LOG.debug("Metadata records deleted. recordsDeleted={} metadataFolder={} duration={}",
                    total, prefix, (System.currentTimeMillis() - start));
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean metadataRecordExists(@NotNull final String name) {
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            boolean exists = objectExists(addMetaKeyPrefix(name));
            LOG.debug("Metadata record [{}] {}. duration={}",
                    name,
                    exists ? "exists" : "requested, but does not exist",
                    (System.currentTimeMillis() - start));
            return exists;
        }
        catch (DataStoreException e) {
            LOG.info("Error checking for existence of metadata record [{}]", name, e);
        }
        finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    protected static String addMetaKeyPrefix(String key) {
        return Utils.META_KEY_PREFIX + key;
    }

    protected static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(Utils.META_KEY_PREFIX)) {
            return name.substring(Utils.META_KEY_PREFIX.length());
        }
        return name;
    }


    // Direct Binary Upload

    @Override
    public void setHttpDownloadURIExpirySeconds(int seconds) {
        httpDownloadURIExpirySeconds = seconds;
    }

    protected int getHttpDownloadURIExpirySeconds() {
        return httpDownloadURIExpirySeconds;
    }

    @Override
    public void setHttpUploadURIExpirySeconds(int seconds) {
        httpUploadURIExpirySeconds = seconds;
    }

    protected int getHttpUploadURIExpirySeconds() {
        return httpUploadURIExpirySeconds;
    }

    @Override
    public void setHttpDownloadURICacheSize(int maxSize) {
        // max size 0 or smaller is used to turn off the cache
        if (maxSize > 0) {
            LOG.info("presigned GET URI cache enabled, maxSize = {} items, expiry = {} seconds", maxSize, httpDownloadURIExpirySeconds / 2);
            httpDownloadURICache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    // cache for half the expiry time of the URIs before giving out new ones
                    .expireAfterWrite(httpDownloadURIExpirySeconds / 2, TimeUnit.SECONDS)
                    .build();
        } else {
            LOG.info("presigned GET URI cache disabled");
            httpDownloadURICache = null;
        }
    }

    @Nullable
    @Override
    public URI createHttpDownloadURI(@NotNull final DataIdentifier identifier,
                                     @NotNull final DataRecordDownloadOptions downloadOptions) {
        if (httpDownloadURIExpirySeconds <= 0) {
            // feature disabled
            return null;
        }

        URI uri = null;
        // if cache is enabled, check the cache
        if (null != httpDownloadURICache) {
            uri = httpDownloadURICache.getIfPresent(identifier);
        }
        if (null == uri) {
            uri = createPresignedGetURI(identifier, downloadOptions);
        }
        if (null != uri && null != httpDownloadURICache) {
            httpDownloadURICache.put(identifier, uri);
        }

        return uri;
    }

    @NotNull
    @Override
    public DataRecord completeHttpUpload(@NotNull final String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException {
        if (Strings.isNullOrEmpty(uploadTokenStr)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        DataRecordUploadToken uploadToken = DataRecordUploadToken.fromEncodedToken(uploadTokenStr, getOrCreateReferenceKey());
        String key = uploadToken.getBlobId();
        DataIdentifier blobId = new DataIdentifier(getIdentifierName(key));
        if (uploadToken.getUploadId().isPresent()) {
            // Existence of an upload token means this is a multi-part upload
            completeMultiPartUpload(uploadToken, key);
        }
        else {
            completeSinglePutUpload(uploadToken, key);
        }

        if (! exists(blobId)) {
            throw new DataRecordUploadException(
                    String.format("Unable to finalize direct write of binary %s", key)
            );
        }

        return getRecord(blobId);
    }

    /**
     * Retrieves the data store reference key for this data store, creating it
     * if one does not exist already.
     *
     * This key is unique per data store.  It can be used to identify content
     * unique to this data store or to sign content created by this data store.
     *
     * @return The reference key
     * @throws DataStoreException if not able to get or create the key.
     */
    @NotNull
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

    /**
     * Generates a random {@link DataIdentifier} that can be used as a blob ID.
     *
     * The standard and still currently preferred way to create a blob ID is to
     * compute a content hash of the blob and use the content hash as the blob
     * ID.  This ties the uniqueness of the hash to the content which also
     * allows us to easily deduplicate blobs.
     *
     * However, if direct binary upload is performed, a content hash cannot be
     * calculated since the content does not flow through Oak.  In such cases
     * it is acceptable to generate a random identifier for the content using
     * this function.  This is done by generating a UUID, which has 2^122
     * different possible values, and then appending the current epoch
     * miilliseconds to the UUID to virtually guarantee uniqueness.
     *
     * Users of this function should be aware that blobs stored using these
     * generated identifiers will not be the same for two blobs with identical
     * content, and thus are not suitable for deduplication.
     *
     * @return A random, unique {@link DataIdentifier}.
     */
    @NotNull
    protected DataIdentifier generateSafeRandomIdentifier() {
        return new DataIdentifier(
                String.format("%s-%d",
                        UUID.randomUUID().toString(),
                        Instant.now().toEpochMilli()
                )
        );
    }

    /**
     * Verifies that a direct binary upload using the given client-provided
     * parameters can be fulfilled using the system constraints.  {@link
     * IllegalArgumentException} will be thrown if the upload cannot be
     * completed as specified.
     *
     * This function is called by {@link org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider#initiateDataRecordUpload(long, int)}
     * to verify that an upload can be completed.
     *
     * @param maxUploadSizeInBytes - As specified by the calling client, the
     *                             expected maximum size of the data to be
     *                             uploaded.
     * @param maxNumberOfURIs - As specified by the calling client, the maximum
     *                        number of URIs the client is willing to accept.
     * @param maxSinglePutUploadSize - The maximum size of a non-multi-part
     *                               (i.e. single-put) upload.  This is a
     *                               cloud service provider implementation
     *                               limitation.
     * @param maxBinaryUploadSize - The maximum size of an uploaded binary.
     *                            This is a cloud service provider
     *                            implementation limitation.
     * @throws IllegalArgumentException if the upload cannot be completed with
     * the specified parameters.
     */
    protected void verifyDirectUpload(long maxUploadSizeInBytes,
                                      int maxNumberOfURIs,
                                      long maxSinglePutUploadSize,
                                      long maxBinaryUploadSize)
            throws IllegalArgumentException {
        if (0L >= maxUploadSizeInBytes) {
            throw new IllegalArgumentException("maxUploadSizeInBytes must be > 0");
        }
        else if (0 == maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (-1 > maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        }
        else if (maxUploadSizeInBytes > maxSinglePutUploadSize &&
                maxNumberOfURIs == 1) {
            throw new IllegalArgumentException(
                    String.format("Cannot do single-put upload with file size %d - exceeds max single-put upload size of %d",
                            maxUploadSizeInBytes,
                            maxSinglePutUploadSize)
            );
        }
        else if (maxUploadSizeInBytes > maxBinaryUploadSize) {
            throw new IllegalArgumentException(
                    String.format("Cannot do upload with file size %d - exceeds max upload size of %d",
                            maxUploadSizeInBytes,
                            maxBinaryUploadSize)
            );
        }
    }

    /**
     * This function is called by {@link org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider#initiateDataRecordUpload(long, int)}
     * to calculate the number of upload parts that should be used for a
     * multi-part upload.
     *
     * @param maxUploadSizeInBytes - As specified by the calling client, the
     *                             expected maximum size of the data to be
     *                             uploaded.
     * @param maxNumberOfURIsRequested - As specified by the calling client, the
     *                                 maximum number of URIs the client is
     *                                 willing to accept.
     * @param maxNumberOfURIsSupported - The maximum number of URIs that can be
     *                                 used in a multi-part upload.  This is a
     *                                 cloud service provider implementation
     *                                 limitation.
     * @param minPartSize - The minimum size of a multi-part upload part.  This
     *                    is a cloud service provider implementation limitation.
     * @param maxPartSize - The maximum size of a multi-part upload part.  This
     *                    is a cloud service provider implementation limitation.
     * @return The number of parts to use for a multi-part upload.
     * @throws IllegalArgumentException if the upload cannot be completed using
     * the specified parameters.
     */
    protected long getNumUploadParts(long maxUploadSizeInBytes,
                                     long maxNumberOfURIsRequested,
                                     long maxNumberOfURIsSupported,
                                     long minPartSize,
                                     long maxPartSize) {
        long numParts;
        if (maxNumberOfURIsRequested > 0) {
            long requestedPartSize = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) maxNumberOfURIsRequested));
            if (requestedPartSize <= maxPartSize) {
                numParts = Math.min(
                        maxNumberOfURIsRequested,
                        Math.min(
                                (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) minPartSize)),
                                maxNumberOfURIsSupported
                        )
                );
            } else {
                throw new IllegalArgumentException(
                        String.format("Cannot do multi-part upload with requested part size %d", requestedPartSize)
                );
            }
        }
        else {
            long maximalNumParts = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) minPartSize));
            numParts = Math.min(maximalNumParts, maxNumberOfURIsSupported);
        }
        return numParts;
    }

    /**
     * Create a {@link DataRecordUpload} object representing a direct binary
     * upload.
     *
     * @param blobId The blobId to be used in the upload token.
     * @param uploadId The uploadId to be used in the upload token.  Can be
     *                 {@code null}.
     * @param minPartSize The minimum acceptable size of an upload part.
     * @param maxPartSize The maximum acceptable size of an upload part.
     * @param uploadPartURIs A collection of URIs for uploading.
     * @return A {@link DataRecordUpload} or null if one can't be created.
     *
     * @see DataRecordUpload
     */
    @Nullable
    protected DataRecordUpload createDataRecordUpload(@NotNull final String blobId,
                                                      @Nullable final String uploadId,
                                                      long minPartSize,
                                                      long maxPartSize,
                                                      @NotNull final Collection<URI> uploadPartURIs) {
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


    protected interface BlobAttributes {
        long getLength();
        long getLastModifiedTime();
    }
}
