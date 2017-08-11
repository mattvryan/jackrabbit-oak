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

package org.apache.jackrabbit.oak.blob.composite;

import java.util.Optional;
import java.util.Properties;

public class DelegateDataStoreSpec {
    private enum DataStoreName {
        INVALID(""),
        FILE_DATA_STORE("FileDataStore"),
        S3_DATA_STORE("S3DataStore"),
        AZURE_DATA_STORE("AzureDataStore");

        private final String name;

        DataStoreName(final String s) {
            name = s;
        }

        @Override
        public String toString() {
            return name;
        }

        public static DataStoreName fromString(final String name) {
            if (null != name) {
                for (DataStoreName dsName : DataStoreName.values()) {
                    if (dsName.name.equalsIgnoreCase(name)) {
                        return dsName;
                    }
                }
            }
            return INVALID;
        }
    }

    private enum DataStoreClassName {
        INVALID(""),
        FILE_DATA_STORE("org.apache.jackrabbit.oak.plugins.blob.datastore.OakCachingFDS"),
        S3_DATA_STORE("org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore"),
        AZURE_DATA_STORE("org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore");

        private final String name;

        DataStoreClassName(final String s) {
            name = s;
        }

        @Override
        public String toString() {
            return name;
        }

        public static DataStoreClassName fromString(final String name) {
            if (null != name) {
                for (DataStoreClassName className : DataStoreClassName.values()) {
                    if (className.name.equalsIgnoreCase(name)) {
                        return className;
                    }
                }
            }
            return INVALID;
        }
    }

    private enum DataStoreBundleName {
        INVALID(""),
        FILE_DATA_STORE("org.apache.jackrabbit.oak-core"),
        S3_DATA_STORE("org.apache.jackrabbit.oak-blob-cloud"),
        AZURE_DATA_STORE("org.apache.jackrabbit.oak-blob-cloud-azure");

        private final String name;

        DataStoreBundleName(final String s) {
            name = s;
        }

        @Override
        public String toString() {
            return name;
        }

        public static DataStoreBundleName fromString(final String name) {
            if (null != name) {
                for (DataStoreBundleName bundleName : DataStoreBundleName.values()) {
                    if (bundleName.name.equalsIgnoreCase(name)) {
                        return bundleName;
                    }
                }
            }
            return INVALID;
        }
    }

    private final DataStoreName dataStoreName;
    private final DataStoreClassName className;
    private final DataStoreBundleName bundleName;
    private final Properties properties;
    private final boolean readOnly;
    private final boolean coldStorage;
    private final Optional<DelegateDataStoreFilter> filter;

    private DelegateDataStoreSpec(final DataStoreName dataStoreName,
                                  final DataStoreClassName className,
                                  final DataStoreBundleName bundleName,
                                  final Properties properties,
                                  final boolean readOnly,
                                  final boolean coldStorage,
                                  final DelegateDataStoreFilter filter) {
        this.dataStoreName = dataStoreName;
        this.className = className;
        this.bundleName = bundleName;
        this.properties = properties;
        this.readOnly = readOnly;
        this.coldStorage = coldStorage;
        this.filter = Optional.ofNullable(filter);
    }

    static Optional<DelegateDataStoreSpec> createFromProperties(final Properties properties) {
        DataStoreName dsName = DataStoreName.fromString(properties.getProperty("dataStoreName"));
        DataStoreClassName className = DataStoreClassName.fromString(properties.getProperty("className"));
        DataStoreBundleName bundleName = DataStoreBundleName.fromString(properties.getProperty("bundleName"));

        if (DataStoreName.INVALID != dsName &&
                DataStoreClassName.INVALID != className &&
                DataStoreBundleName.INVALID != bundleName) {
            if ( (DataStoreName.FILE_DATA_STORE == dsName &&
                    DataStoreClassName.FILE_DATA_STORE == className &&
                    DataStoreBundleName.FILE_DATA_STORE == bundleName) ||
                    (DataStoreName.S3_DATA_STORE == dsName &&
                            DataStoreClassName.S3_DATA_STORE == className &&
                            DataStoreBundleName.S3_DATA_STORE == bundleName) ||
                    (DataStoreName.AZURE_DATA_STORE == dsName &&
                            DataStoreClassName.AZURE_DATA_STORE == className &&
                            DataStoreBundleName.AZURE_DATA_STORE == bundleName)) {
                return Optional.of(new DelegateDataStoreSpec(
                        dsName,
                        className,
                        bundleName,
                        properties,
                        Boolean.getBoolean(properties.getProperty("readOnly", "false")),
                        Boolean.getBoolean(properties.getProperty("coldStorage", "false")),
                        DelegateDataStoreFilter.create((String)properties.get("filter"))
                ));
            }
            else {
                return Optional.empty();
            }
        }
        else
        {
            return Optional.empty();
        }
    }

    public String getDataStoreName() {
        return dataStoreName.name;
    }

    public String getClassName() {
        return className.name;
    }

    public String getBundleName() {
        return bundleName.name;
    }

    public Properties getProperties() {
        return properties;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isColdStorage() {
        return coldStorage;
    }

    public boolean hasFilter() {
        return filter.isPresent();
    }

    public DelegateDataStoreFilter getFilter() {
        return filter.orElse(DelegateDataStoreFilter.emptyFilter());
    }
}
