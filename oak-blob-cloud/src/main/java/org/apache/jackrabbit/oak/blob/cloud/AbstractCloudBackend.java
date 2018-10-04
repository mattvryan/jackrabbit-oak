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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
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

    private Properties properties;

    @Override
    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    protected Properties getProperties() {
        return properties;
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

    protected static String addMetaKeyPrefix(String key) {
        return Utils.META_KEY_PREFIX + key;
    }

    protected static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(Utils.META_KEY_PREFIX)) {
            return name.substring(Utils.META_KEY_PREFIX.length());
        }
        return name;
    }

    protected interface BlobAttributes {
        long getLength();
        long getLastModifiedTime();
    }
}
