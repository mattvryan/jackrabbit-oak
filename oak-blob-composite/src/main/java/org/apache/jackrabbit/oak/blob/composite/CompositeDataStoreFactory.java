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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreFactory;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreServiceRegistrar;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.service.component.ComponentContext;

import javax.jcr.RepositoryException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookup;

public class CompositeDataStoreFactory implements DataStoreFactory {
    private static final String DATASTORE_PRIMARY = "datastore.primary";
    private static final String DATASTORE_SECONDARY = "datastore.secondary";

    @Override
    public DataStore createDataStore(ComponentContext context,
                                     Map<String, Object> config,
                                     String[] dataStoreServiceDescription,
                                     DataStoreServiceRegistrar registrar,
                                     StatisticsProvider statisticsProvider,
                                     boolean useJR2Caching) throws RepositoryException {
        Properties properties = new Properties();

        CompositeDataStore dataStore = new CompositeDataStore();

        // Register a bundle listener to create data stores for any specified in config whose bundles
        // will be activated later.
        // This listener will also shut down any active data stores if their corresponding bundle is
        // shut down.
        context.getBundleContext().addBundleListener(dataStore);
        // This listener will also log any data stores that didn't get set up by the time the framework is started.
        context.getBundleContext().addFrameworkListener(dataStore);

        // Parse config to get a list of all the data stores we want to use.
        for (Map.Entry<String, Object> entry: config.entrySet()) {
            if (DATASTORE_PRIMARY.equals(entry.getKey())) {
                Properties dsProps = parseProperties((String)entry.getValue());
                if (null == dsProps.get("path")) { dsProps.put("path", "/repository/datastore/primary"); }
                dsProps.put("homeDir", lookup(context, "repository.home"));
                dataStore.setDefaultDataStoreSpecification(dsProps);
            }
            else if (DATASTORE_SECONDARY.equals(entry.getKey())){
                Properties dsProps = parseProperties((String)entry.getValue());
                if (null == dsProps.get("path")) { dsProps.put("path", "/repostory/datastore/secondary"); }
                dsProps.put("homeDir", lookup(context, "repository.home"));
                dataStore.addSecondaryDataStoreSpecification(dsProps);
            }
            else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        dataStore.setProperties(properties);

        dataStore.addActiveDataStores(context.getBundleContext());

        return dataStore;
    }

    private Properties parseProperties(final String props) {
        Map<String, Object> cfgMap = Maps.newHashMap();
        List<String> cfgPairs = Lists.newArrayList(props.split(","));
        for (String s : cfgPairs) {
            String[] parts = s.split(":");
            if (parts.length != 2) continue;
            cfgMap.put(parts[0], parts[1]);
        }
        Properties properties = new Properties();
        properties.putAll(cfgMap);
        return properties;
    }
}
