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

import java.net.URI;
import java.util.Properties;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface CloudBackend {
    void setProperties(@NotNull final Properties properties);

    void setHttpUploadURIExpirySeconds(int seconds);
    void setBinaryTransferAccelerationEnabled(boolean enabled);
    void setHttpDownloadURIExpirySeconds(int seconds);
    void setHttpDownloadURICacheSize(int maxSize);

    @Nullable DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes,
                                                  int maxNumberOfURIs);
    @NotNull DataRecord completeHttpUpload(@NotNull String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException;
    @Nullable URI createHttpDownloadURI(@NotNull DataIdentifier identifier,
                                        @NotNull DataRecordDownloadOptions downloadOptions);
}
