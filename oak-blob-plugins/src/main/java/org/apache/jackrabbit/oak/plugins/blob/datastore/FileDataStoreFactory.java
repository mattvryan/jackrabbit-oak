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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import com.google.common.base.Preconditions;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.Constants;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

public class FileDataStoreFactory implements DataStoreFactory {
    private static final String DESCRIPTION = "oak.datastore.description";

    public static final String CACHE_PATH = "cachePath";
    public static final String CACHE_SIZE = "cacheSize";
    public static final String FS_BACKEND_PATH = "fsBackendPath";
    public static final String PATH = "path";
    public static final String ASYNC_UPLOAD_LIMIT = "asyncUploadLimit";

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public DataStore createDataStore(ComponentContext context,
                                     Map<String, Object> config,
                                     String[] dataStoreServiceDescription,
                                     DataStoreServiceRegistrar registrar,
                                     StatisticsProvider statisticsProvider,
                                     boolean useJR2Caching) throws RepositoryException {

        long cacheSize = PropertiesUtil.toLong(config.get(CACHE_SIZE), 0L);

        // return CachingFDS when cacheSize > 0
        if (cacheSize > 0) {
            String fsBackendPath = PropertiesUtil.toString(config.get(PATH), null);
            Preconditions.checkNotNull(fsBackendPath, "Cannot create " +
                    "FileDataStoreService with caching. [{path}] property not configured.");

            config.remove(PATH);
            config.remove(CACHE_SIZE);
            config.put(FS_BACKEND_PATH, fsBackendPath);
            config.put("cacheSize", cacheSize);
            String cachePath = PropertiesUtil.toString(config.get(CACHE_PATH), null);
            if (cachePath != null) {
                config.remove(CACHE_PATH);
                config.put(PATH, cachePath);
            }
            Properties properties = new Properties();
            properties.putAll(config);
            log.info("Initializing with properties " + properties);

            if (useJR2Caching) {
                return createOakCachingFileDataStore(config, properties);
            }
            else {
                return createCachingFileDataStore(properties, dataStoreServiceDescription, registrar);
            }
        } else {
            log.info("OakFileDataStore initialized");
            return new OakFileDataStore();
        }
    }

    private OakCachingFDS createOakCachingFileDataStore(final Map<String, Object> config, final Properties properties) {
        OakCachingFDS dataStore = new OakCachingFDS();
        dataStore.setFsBackendPath((String) config.get(FS_BACKEND_PATH));
        // Disabling asyncUpload by default
        dataStore.setAsyncUploadLimit(
                PropertiesUtil.toInteger(config.get(ASYNC_UPLOAD_LIMIT), 0));
        dataStore.setProperties(properties);
        log.info("OakCachingFDS initialized");
        return dataStore;
    }

    private CachingFileDataStore createCachingFileDataStore(final Properties properties,
                                                            final String[] dataStoreServiceDescription,
                                                            final DataStoreServiceRegistrar registrar) {
        CachingFileDataStore dataStore = new CachingFileDataStore();
        dataStore.setStagingSplitPercentage(
                PropertiesUtil.toInteger(properties.get("stagingSplitPercentage"), 0));
        dataStore.setProperties(properties);
        Dictionary<String, Object> regConfig = new Hashtable<String, Object>();
        regConfig.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        regConfig.put(DESCRIPTION, dataStoreServiceDescription);

        if (null != registrar) {
            registrar.setClassNames(new String[]{
                    AbstractSharedCachingDataStore.class.getName(),
                    AbstractSharedCachingDataStore.class.getName()});
            registrar.setConfig(regConfig);
        }

        log.info("CachingFileDataStore initialized");
        return dataStore;

    }
}
