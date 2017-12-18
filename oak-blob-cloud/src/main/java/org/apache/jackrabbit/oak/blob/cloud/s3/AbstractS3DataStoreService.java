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

package org.apache.jackrabbit.oak.blob.cloud.s3;

import com.google.common.io.Closer;
import org.apache.felix.scr.annotations.Component;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

@Component(componentAbstract = true)
public abstract class AbstractS3DataStoreService extends AbstractDataStoreService {
    private static final String DESCRIPTION = "oak.datastore.description";

    protected Closer closer = Closer.create();

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        return createS3DataStore(context, config, getStatisticsProvider(), getDescription(), closer);
    }

    static DataStore createS3DataStore(ComponentContext context,
                                       Map<String, Object> config,
                                       StatisticsProvider statisticsProvider,
                                       String[] description,
                                       Closer closer) {
        Properties properties = new Properties();
        properties.putAll(config);

        S3DataStore dataStore = new S3DataStore();
        dataStore.setStatisticsProvider(statisticsProvider);
        dataStore.setProperties(properties);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, description);

        closer.register(asCloseable(
                context.getBundleContext().registerService(new String[] {
                        AbstractSharedCachingDataStore.class.getName(),
                        AbstractSharedCachingDataStore.class.getName()
                }, dataStore, props)

        ));

        return dataStore;
    }

    protected void deactivate() throws DataStoreException {
        if (null != closer) {
            closeQuietly(closer);
        }
        super.deactivate();
    }

    @Override
    protected String[] getDescription() {
        return new String[] {"type=S3"};
    }

    private static Closeable asCloseable(final ServiceRegistration registration) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                registration.unregister();
            }
        };
    }
}
