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
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreFactory;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreServiceRegistrar;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

@Component(componentAbstract = true)
public abstract class AbstractCompositeDataStoreService extends AbstractDataStoreService {
    private static Logger LOG = LoggerFactory.getLogger(AbstractCompositeDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";

    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) throws RepositoryException {
        DataStoreServiceRegistrar registrar = new DataStoreServiceRegistrar(context.getBundleContext());
        registrar.setClassNames(new String[] { DataStore.class.getName(), CompositeDataStore.class.getName() });
        DataStore dataStore = getDataStoreFactory().createDataStore(
                context,
                config,
                getDescription(),
                registrar,
                getStatisticsProvider(),
                JR2_CACHING
        );

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());
        registrar.setConfig(props);

        if (registrar.needsDelegateRegistration()) {
            delegateReg = registrar.createDelegateRegistration(dataStore);
        }
        return dataStore;
    }

    protected void deactivate() throws DataStoreException {
        if (delegateReg != null) {
            delegateReg.unregister();
        }
        super.deactivate();
    }

    @Override
    protected DataStoreFactory getDataStoreFactory() {
        return new CompositeDataStoreFactory();
    }

    @Override
    protected String[] getDescription() {
        return new String[] {"type=CompositeBlob"};
    }
}
