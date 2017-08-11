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

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreFactory;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreServiceRegistrar;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.service.component.ComponentContext;

import javax.jcr.RepositoryException;
import java.util.Map;

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

        CompositeDataStore dataStore = new CompositeDataStore();

        // Register a bundle listener to create data stores for any specified in config whose bundles
        // will be activated later.
        // This listener will also shut down any active data stores if their corresponding bundle is
        // shut down.
        context.getBundleContext().addBundleListener(dataStore);
        // This listener will also log any data stores that didn't get set up by the time the framework is started.
        context.getBundleContext().addFrameworkListener(dataStore);


        dataStore.setProperties(config, context);
        dataStore.addActiveDataStores(context.getBundleContext());

        return dataStore;
    }
}
