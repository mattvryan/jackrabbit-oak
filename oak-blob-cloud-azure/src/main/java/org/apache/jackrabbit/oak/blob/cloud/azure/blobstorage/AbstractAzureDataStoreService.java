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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreFactory;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreServiceRegistrar;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

import javax.jcr.RepositoryException;
import java.util.Map;

public abstract class AbstractAzureDataStoreService extends AbstractDataStoreService {
    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) throws RepositoryException {
        DataStoreServiceRegistrar registrar = new DataStoreServiceRegistrar(context.getBundleContext());
        DataStore dataStore = getDataStoreFactory().createDataStore(context,
                config,
                getDescription(),
                registrar,
                getStatisticsProvider(),
                JR2_CACHING);

        return dataStore;
    }

    @Override
    protected DataStoreFactory getDataStoreFactory() {
        return new AzureDataStoreFactory();
    }

    protected void deactivate() throws DataStoreException {
        if (delegateReg != null) {
            delegateReg.unregister();
        }
        super.deactivate();
    }

    @Override
    protected String[] getDescription() {
        return new String[] {"type=AzureBlob"};
    }
}
