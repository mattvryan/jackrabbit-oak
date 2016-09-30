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

import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.CachingDataStore;

import java.util.Properties;

/**
 * A data store that uses Azure Blob Store.
 */
public class AzureDataStore extends CachingDataStore {

    protected Properties properties;

    @Override
    protected Backend createBackend() {
        AzureBlobStoreBackend backend = new AzureBlobStoreBackend();
        if (null != properties) {
            backend.setProperties(properties);
        }
        return backend;
    }

    /**
     * Properties required to configure the AzureBlobStoreBackend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    protected String getMarkerFile() {
        return "azure-blob-store.init.done";
    }
}
