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

package org.apache.jackrabbit.oak.blob.composite.config;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class DelegateDataStoreConfig {
    enum DataStoreName {
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

//    enum DataStoreClassName {
//        INVALID(""),
//        FILE_DATA_STORE("org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore"),
//        S3_DATA_STORE("org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore"),
//        AZURE_DATA_STORE("");
//
//        private final String name;
//
//        DataStoreClassName(final String s) {
//            name = s;
//        }
//
//        @Override
//        public String toString() {
//            return name;
//        }
//
//        public static DataStoreClassName fromString(final String name) {
//            if (null != name) {
//                for (DataStoreClassName className : DataStoreClassName.values()) {
//                    if (className.name.equalsIgnoreCase(name)) {
//                        return className;
//                    }
//                }
//            }
//            return INVALID;
//        }
//    }
//
//    enum DataStoreBundleName {
//        INVALID(""),
//        FILE_DATA_STORE("org.apache.jackrabbit.oak-core"),
//        S3_DATA_STORE("org.apache.jackrabbit.oak-blob-cloud"),
//        AZURE_DATA_STORE("org.apache.jackrabbit.oak-blob-cloud-azure");
//
//        private final String name;
//
//        DataStoreBundleName(final String s) {
//            name = s;
//        }
//
//        @Override
//        public String toString() {
//            return name;
//        }
//
//        public static DataStoreBundleName fromString(final String name) {
//            if (null != name) {
//                for (DataStoreBundleName bundleName : DataStoreBundleName.values()) {
//                    if (bundleName.name.equalsIgnoreCase(name)) {
//                        return bundleName;
//                    }
//                }
//            }
//            return INVALID;
//        }
//    }

//    static final Map<DataStoreName, DataStoreClassName> dsClassNames;
//    static {
//        Map<DataStoreName, DataStoreClassName> m = Maps.newHashMap();
//        m.put(DataStoreName.FILE_DATA_STORE, DataStoreClassName.FILE_DATA_STORE);
//        m.put(DataStoreName.S3_DATA_STORE, DataStoreClassName.S3_DATA_STORE);
//        m.put(DataStoreName.AZURE_DATA_STORE, DataStoreClassName.AZURE_DATA_STORE);
//        dsClassNames = Collections.unmodifiableMap(m);
//    }
//    static final Map<DataStoreName, DataStoreBundleName> dsBundleNames;
//    static {
//        Map<DataStoreName, DataStoreBundleName> m = Maps.newHashMap();
//        m.put(DataStoreName.FILE_DATA_STORE, DataStoreBundleName.FILE_DATA_STORE);
//        m.put(DataStoreName.S3_DATA_STORE, DataStoreBundleName.S3_DATA_STORE);
//        m.put(DataStoreName.AZURE_DATA_STORE, DataStoreBundleName.AZURE_DATA_STORE);
//        dsBundleNames = Collections.unmodifiableMap(m);
//    }

    private final DataStoreName dataStoreName;
//    private final DataStoreClassName className;
//    private final DataStoreBundleName bundleName;
    private final String role;
    private final Properties properties;
    private final boolean readOnly;
    //private final boolean coldStorage;
    //private final Optional<DelegateDataStoreFilter> filter;

    private DelegateDataStoreConfig(final DataStoreName dataStoreName,
//                                    final DataStoreClassName className,
//                                    final DataStoreBundleName bundleName,
                                    final String role,
                                    final Properties properties,
                                    final boolean readOnly) {
        //final boolean coldStorage,
        //final DelegateDataStoreFilter filter) {
        this.dataStoreName = dataStoreName;
//        this.className = className;
//        this.bundleName = bundleName;
        this.role = role;
        this.properties = properties;
        this.readOnly = readOnly;
        //this.coldStorage = coldStorage;
        //this.filter = Optional.ofNullable(filter);
    }

    static Optional<DelegateDataStoreConfig> createFromString(final String cfg) {
        if (Strings.isNullOrEmpty(cfg)) {
            return Optional.empty();
        }

        Map<String, Object> cfgMap = Maps.newHashMap();
        List<String> cfgPairs = Lists.newArrayList(cfg.split(","));
        for (String s : cfgPairs) {
            String[] parts = s.split(":");
            if (parts.length != 2) continue;
            cfgMap.put(parts[0], parts[1]);
        }
        Properties properties = new Properties();
        properties.putAll(cfgMap);
        return createFromProperties(properties);
    }

    static Optional<DelegateDataStoreConfig> createFromProperties(final Properties properties) {
        if (null == properties) {
            return Optional.empty();
        }

        DataStoreName dsName = DataStoreName.fromString(properties.getProperty("dataStoreName"));
        String role = properties.getProperty("role", null);
//        DataStoreClassName className = dsClassNames.getOrDefault(dsName, DataStoreClassName.INVALID);
//        DataStoreBundleName bundleName = dsBundleNames.getOrDefault(dsName, DataStoreBundleName.INVALID);

        if (DataStoreName.INVALID != dsName &&
//                DataStoreClassName.INVALID != className &&
//                DataStoreBundleName.INVALID != bundleName
                ! Strings.isNullOrEmpty(role)
                ) {
            if (! properties.containsKey("homeDir")) {
                if (properties.containsKey("repository.home")) {
                    properties.put("homeDir", properties.getProperty("repository.home"));
                } else if (properties.containsKey("path")) {
                    properties.put("homeDir", properties.getProperty("path"));
                }
            }
            return Optional.of(new DelegateDataStoreConfig(
                    dsName,
//                    className,
//                    bundleName,
                    role,
                    properties,
                    Boolean.parseBoolean(properties.getProperty("readOnly", "false"))
            ));
        }
        else
        {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        StringBuilder output = new StringBuilder();
        output.append("dataStoreName:");
        output.append(dataStoreName);
        output.append(",role:");
        output.append(role);
        for (String propertyName : properties.stringPropertyNames()) {
            if (propertyName.equals("dataStoreName") ||
                    propertyName.equals("role") ||
                    propertyName.equals("readOnly")) {
                continue;
            }
            output.append(String.format(",%s:%s", propertyName, properties.getProperty(propertyName)));
        }
        if (isReadOnly()) {
            output.append(",readOnly:true");
        }
        return output.toString();
    }

    public String getDataStoreName() {
        return dataStoreName.name;
    }

//    public String getClassName() {
//        return className.name;
//    }
//
//    public String getBundleName() {
//        return bundleName.name;
//    }

    public String getRole() {
        return role;
    }

    public Properties getProperties() {
        return properties;
    }

    public Object getProperty(final String key) {
        return properties.get(key);
    }

    public void addProperty(final Object key, final Object value) {
        properties.put(key, value);
    }

    public void removeProperty(final Object key) {
        properties.remove(key);
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
