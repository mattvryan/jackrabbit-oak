/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.abs.AzureDataStore;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

@Component(policy = ConfigurationPolicy.REQUIRE, name = AzureDataStoreService.NAME)
public class AzureDataStoreService extends AbstractDataStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.AzureDataStore";
    private static final String DESCRIPTION = "oak.datastore.description";

    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        AzureDataStore dataStore = new AzureDataStore();

        Properties properties = new Properties();
        properties.putAll(config);

        dataStore.setProperties(properties);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());

        delegateReg = context.getBundleContext().registerService(new String[] {
                AzureDataStore.class.getName(),
                AzureDataStore.class.getName()
        }, dataStore , props);

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
        return new String[] {"type=AzureBlobStore"};
    }
}
