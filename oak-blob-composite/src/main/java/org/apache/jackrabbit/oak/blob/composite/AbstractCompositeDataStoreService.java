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

import org.apache.felix.scr.annotations.Component;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

@Component(componentAbstract = true)
public abstract class AbstractCompositeDataStoreService extends AbstractDataStoreService {
    private static Logger LOG = LoggerFactory.getLogger(AbstractCompositeDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";

    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        CompositeDataStore dataStore = new CompositeDataStore();

        BundleContext bundleContext = context.getBundleContext();

        // Register a bundle listener to create data stores for any specified in config whose bundles
        // will be activated later.
        // This listener will also shut down any active data stores if their corresponding bundle is
        // shut down.
        bundleContext.addBundleListener(dataStore);
        // This listener will also log any data stores that didn't get set up by the time the framework is started.
        bundleContext.addFrameworkListener(dataStore);


        dataStore.setProperties(config, context);

        dataStore.addActiveDataStores(context.getBundleContext());

        Dictionary<String, Object> props = new Hashtable<>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());

        delegateReg = bundleContext.registerService(
                new String[] {
                        DataStore.class.getName(),
                        CompositeDataStore.class.getName()
                },
                dataStore,
                props);

        return dataStore;
    }

    protected void deactivate() throws DataStoreException {
        if (delegateReg != null) {
            delegateReg.unregister();
        }
        super.deactivate();
    }

    @Override
    protected String[] getDescription() {
        return new String[] {"type=CompositeBlob"};
    }
}