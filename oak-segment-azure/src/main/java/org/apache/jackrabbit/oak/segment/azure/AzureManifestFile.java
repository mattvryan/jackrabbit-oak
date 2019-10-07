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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.azure.storage.blob.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureManifestFile implements ManifestFile {

    private static final Logger log = LoggerFactory.getLogger(AzureManifestFile.class);

//    private final CloudBlockBlob manifestBlob;

    private final BlockBlobClient manifestBlob;

//    public AzureManifestFile(CloudBlockBlob manifestBlob) {
//        this.manifestBlob = manifestBlob;
//    }

    public AzureManifestFile(BlockBlobClient manifestBlob) {
        this.manifestBlob = manifestBlob;
    }

    @Override
    public boolean exists() {
//        try {
//            return manifestBlob.exists();
//        } catch (StorageException e) {
//            log.error("Can't check if the manifest exists", e);
//            return false;
//        }
        return manifestBlob.exists();
    }

    @Override
    public Properties load() throws IOException {
//        Properties properties = new Properties();
//        if (exists()) {
//            long length = manifestBlob.getProperties().getLength();
//            byte[] data = new byte[(int) length];
//            try {
//                manifestBlob.downloadToByteArray(data, 0);
//            } catch (StorageException e) {
//                throw new IOException(e);
//            }
//            properties.load(new ByteArrayInputStream(data));
//        }
//        return properties;

        Properties properties = new Properties();
        if (exists()) {
            properties.load(manifestBlob.openInputStream());
        }
        return properties;
    }

    @Override
    public void save(Properties properties) throws IOException {
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        properties.store(bos, null);
//
//        byte[] data = bos.toByteArray();
//        try {
//            manifestBlob.uploadFromByteArray(data, 0, data.length);
//        } catch (StorageException e) {
//            throw new IOException(e);
//        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        properties.store(bos, null);
        byte[] data = bos.toByteArray();
        manifestBlob.upload(new BufferedInputStream(new ByteArrayInputStream(data)), data.length);
    }
}
