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

package org.apache.jackrabbit.oak.segment.azure.compat;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudBlobDirectory {
    private static Logger LOG = LoggerFactory.getLogger(CloudBlobDirectory.class);

    private final ContainerClient client;
    private final String directory;

    public CloudBlobDirectory(@NotNull final ContainerClient client, @NotNull final String directory) {
        this.client = client;
        this.directory = directory;
    }

    public ContainerClient client() { return client; }
    public String directory() { return directory; }

    public PagedIterable<BlobItem> listBlobsFlat() {
        return listBlobsFlat(new ListBlobsOptions().prefix(directory), null);
    }

    public PagedIterable<BlobItem> listBlobsFlat(ListBlobsOptions options, Duration duration) {
        return client.listBlobsHierarchy("/", new ListBlobsOptions().prefix(Paths.get(directory, options.prefix()).toString()), duration);
    }

    public BlobClient getBlobClient(@NotNull final String blobName) {
        return client.getBlobClient(Paths.get(directory, blobName).toString());
    }

    public CloudBlobDirectory getDirectoryReference(@NotNull final String dirName) {
        return new CloudBlobDirectory(client, Paths.get(directory, dirName).toString());
    }

    public void deleteBlobIfExists(BlobClient blob) {
        if (blob.exists()) blob.delete();
    }

    public URI getUri() {
        URL containerUrl = client.getContainerUrl();
        String path = Paths.get(containerUrl.getPath(), directory).toString();
        try {
            return new URI(containerUrl.getProtocol(),
                    containerUrl.getUserInfo(),
                    containerUrl.getHost(),
                    containerUrl.getPort(),
                    path,
                    containerUrl.getQuery(),
                    null);
        }
        catch (URISyntaxException e) {
            LOG.warn("Unable to format directory URI", e);
            return null;
        }
    }
}
