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
package org.apache.jackrabbit.oak.segment.azure;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.StorageException;
import com.azure.storage.common.credentials.SharedKeyCredential;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AzureUtilities {

    public static String SEGMENT_FILE_NAME_PATTERN = "^([0-9a-f]{4})\\.([0-9a-f-]+)$";

    private static final Logger log = LoggerFactory.getLogger(AzureUtilities.class);

    private AzureUtilities() {
    }

    public static String getSegmentFileName(AzureSegmentArchiveEntry indexEntry) {
        return getSegmentFileName(indexEntry.getPosition(), indexEntry.getMsb(), indexEntry.getLsb());
    }

    public static String getSegmentFileName(long offset, long msb, long lsb) {
        return String.format("%04x.%s", offset, new UUID(msb, lsb).toString());
    }

//    public static String getName(CloudBlob blob) {
//        return Paths.get(blob.getName()).getFileName().toString();
//    }

    public static String getName(BlobClient blob) {
        Path blobPath = Paths.get(blob.getBlobUrl().getPath());
        int nElements = blobPath.getNameCount();
        return nElements > 1 ? blobPath.subpath(1, nElements-1).toString() : blobPath.toString();
    }

//    public static String getName(CloudBlobDirectory directory) {
//        return Paths.get(directory.getUri().getPath()).getFileName().toString();
//    }

    public static String getName(CloudBlobDirectory directory) {
        return directory.directory();
    }

//    public static List<CloudBlob> getBlobs(CloudBlobDirectory directory) throws IOException {
//        try {
//            return StreamSupport.stream(directory.listBlobs(null, false, EnumSet.of(BlobListingDetails.METADATA), null, null).spliterator(), false)
//                    .filter(i -> i instanceof CloudBlob)
//                    .map(i -> (CloudBlob) i)
//                    .collect(Collectors.toList());
//        } catch (StorageException | URISyntaxException e) {
//            throw new IOException(e);
//        }
//    }

    public static List<BlobClient> getBlobs(CloudBlobDirectory directory) {
        List<BlobClient> allBlobs = Lists.newArrayList();
        directory.listBlobsFlat()
                .streamByPage()
                .map(blobItemPagedResponse -> allBlobs.addAll(
                        blobItemPagedResponse.value()
                                .stream()
                                .map(blobItem -> directory.getBlobClient(blobItem.name()))
                                .collect(Collectors.toList())
                ));
        return allBlobs;
    }

//    public static void readBufferFully(CloudBlob blob, Buffer buffer) throws IOException {
//        try {
//            blob.download(new ByteBufferOutputStream(buffer));
//            buffer.flip();
//        } catch (StorageException e) {
//            throw new RepositoryNotReachableException(e);
//        }
//    }

    public static void readBufferFully(BlobClient blob, Buffer buffer) throws IOException {
        IOUtils.copy(blob.openInputStream(), new ByteBufferOutputStream(buffer));
        buffer.flip();
    }

//    public static void deleteAllEntries(CloudBlobDirectory directory) throws IOException {
//        getBlobs(directory).forEach(b -> {
//            try {
//                b.deleteIfExists();
//            } catch (StorageException e) {
//                log.error("Can't delete blob {}", b.getUri().getPath(), e);
//            }
//        });
//    }

    public static void deleteAllEntries(CloudBlobDirectory directory) {
        getBlobs(directory).forEach(b -> directory.deleteBlobIfExists(b));
    }

//    public static CloudBlobDirectory cloudBlobDirectoryFrom(StorageCredentials credentials,
//            String uri, String dir) throws URISyntaxException, StorageException {
//        StorageUri storageUri = new StorageUri(new URI(uri));
//        CloudBlobContainer container = new CloudBlobContainer(storageUri, credentials);
//
//        return container.getDirectoryReference(dir);
//    }
//
//    public static CloudBlobDirectory cloudBlobDirectoryFrom(String connection, String containerName,
//            String dir) throws InvalidKeyException, URISyntaxException, StorageException {
//        CloudStorageAccount cloud = CloudStorageAccount.parse(connection);
//        CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(containerName);
//        container.createIfNotExists();
//
//        return container.getDirectoryReference(dir);
//    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(SharedKeyCredential credential, String uriString, String dir)
            throws URISyntaxException {
        URI uri = new URI(uriString);
        String host = uri.getHost();
        String container = Paths.get(uri.getPath()).subpath(0, 1).toString();
        ContainerClient client = new BlobServiceClientBuilder()
                .credential(credential)
                .endpoint(String.format("https://%s", host))
                .buildClient()
                .getContainerClient(container);
        return new CloudBlobDirectory(client, container, dir);
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(String connection, String containerName,
            String dir) throws StorageException {
        String proto = "https";
        String accountName = null;
        String accountKey = null;
        String blobEndpoint = null;
        for (String keypair : connection.split(";")) {
            String[] parts = keypair.split("=", 2);
            if (2 == parts.length) {
                String key = parts[0].toLowerCase();
                if (key.equals(AzureConfigurationParserUtils.AzureConnectionKey.DEFAULT_ENDPOINTS_PROTOCOL.text())) {
                    proto = parts[1];
                }
                else if (key.equals(AzureConfigurationParserUtils.AzureConnectionKey.ACCOUNT_NAME.text())) {
                    accountName = parts[1];
                }
                else if (key.equals(AzureConfigurationParserUtils.AzureConnectionKey.ACCOUNT_KEY.text())) {
                    accountKey = parts[1];
                }
                else if (key.equals(AzureConfigurationParserUtils.AzureConnectionKey.BLOB_ENDPOINT.text())) {
                    blobEndpoint = parts[1];
                }
            }
        }

        if (null != accountName && null != accountKey) {
            SharedKeyCredential credential = new SharedKeyCredential(accountName, accountKey);
            BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder()
                    .credential(credential);
            if (null != blobEndpoint) {
                clientBuilder.endpoint(String.format("%s://%s", proto, blobEndpoint));
            }
            ContainerClient containerClient = clientBuilder.buildClient().getContainerClient(containerName);
            return new CloudBlobDirectory(containerClient, containerName, dir);
        }
        throw new IllegalArgumentException(String.format("Invalid connection string - could not parse '%s'", connection));
    }

    private static class ByteBufferOutputStream extends OutputStream {

        @NotNull
        private final Buffer buffer;

        public ByteBufferOutputStream(@NotNull Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) {
            buffer.put((byte)b);
        }

        @Override
        public void write(@NotNull byte[] bytes, int offset, int length) {
            buffer.put(bytes, offset, length);
        }
    }
}


