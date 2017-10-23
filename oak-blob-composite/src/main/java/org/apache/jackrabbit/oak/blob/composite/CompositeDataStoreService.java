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
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.apache.jackrabbit.oak.spi.blob.BlobStoreProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Component(policy = ConfigurationPolicy.REQUIRE, name = CompositeDataStoreService.NAME)
public class CompositeDataStoreService extends AbstractDataStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.CompositeDataStore";
    private static Logger LOG = LoggerFactory.getLogger(CompositeDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";

    private ServiceRegistration dataStoreReg = null;
    private CompositeDataStore dataStore = null;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_MULTIPLE,
            policy = ReferencePolicy.DYNAMIC,
            bind = "addDelegateDataStore",
            unbind = "removeDelegateDataStore",
            referenceInterface = BlobStoreProvider.class,
            target="(!(service.pid=org.apache.jackrabbit.oak.plugins.blob.datastore.CompositeDataStore))"
    )
    private List<CompositeDataStoreDelegate> delegateDataStores = Lists.newArrayList();
    private Set<BlobStoreProvider> dataStoresInUse = Sets.newConcurrentHashSet();

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        // parse and save configuration here

        registerCompositeDataStore();

        return dataStore;
    }

    private void registerCompositeDataStore() {
        if (null != dataStoreReg) {
            // Already registered
            return;
        }
        if (delegateDataStores.isEmpty()) {
            LOG.info("Composite Data Store registration is deferred until there is an active delegate data store");
            return;
        }

        boolean needToRegisterDataStore = false;
        if (null == dataStore) {
            Properties properties = new Properties();
            properties.putAll(config);
            dataStore = new CompositeDataStore(properties);
            needToRegisterDataStore = true;
        }
        for (CompositeDataStoreDelegate delegate : delegateDataStores) {
            dataStore.addDelegate(delegate);
            dataStoresInUse.add(delegate.getDataStore());
        }

        BundleContext bundleContext = context.getBundleContext();

        Dictionary<String, Object> props = new Hashtable<>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());

        dataStoreReg = bundleContext.registerService(
                new String[] {
                        DataStore.class.getName(),
                        CompositeDataStore.class.getName()
                },
                dataStore,
                props);

        if (needToRegisterDataStore) {
            try {
                registerDataStore(dataStore);
            }
            catch (RepositoryException e) {
                LOG.error("Failed to complete CompositeDataStore registration", e);
                dataStore = null;
            }
        }
    }

    protected void deactivate() throws DataStoreException {
        unregisterCompositeDataStore();
        super.deactivate();
    }

    private void unregisterCompositeDataStore() {
        if (dataStoreReg != null) {
            dataStoreReg.unregister();
        }
    }

    protected void addDelegateDataStore(final BlobStoreProvider ds, final Map<String, ?> props) {
        delegateDataStores.add(new CompositeDataStoreDelegate(ds, props));
        // Should we be able to add delegates even after this service is registered?
        if (context == null) {
            LOG.info("addDelegateDataStore: context is null, delaying reconfiguration");
            return;
        }
        if (dataStoreReg == null) {
            registerCompositeDataStore();
        }
    }

    protected void removeDelegateDataStore(final BlobStoreProvider ds) {
        dataStore.removeDelegate(ds);

        Iterator<CompositeDataStoreDelegate> iter = delegateDataStores.iterator();
        delegateDataStores.removeIf((CompositeDataStoreDelegate delegate) -> delegate.getDataStore() == ds);

        if (context == null) {
            LOG.info("removeDelegateDataStore: context is null, delaying reconfiguration");
            return;
        }

        if (dataStoreReg != null && dataStoresInUse.contains(ds)) {
            unregisterCompositeDataStore();
        }
    }

    @Override
    protected String[] getDescription() {
        return new String[] {"type=CompositeBlob"};
    }
}
