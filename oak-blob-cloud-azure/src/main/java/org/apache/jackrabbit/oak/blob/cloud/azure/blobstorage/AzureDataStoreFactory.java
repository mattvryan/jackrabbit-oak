package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreFactory;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreServiceRegistrar;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.Constants;
import org.osgi.service.component.ComponentContext;

import javax.jcr.RepositoryException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

public class AzureDataStoreFactory implements DataStoreFactory {
    private static final String DESCRIPTION = "oak.datastore.description";

    @Override
    public DataStore createDataStore(ComponentContext context,
                                     Map<String, Object> config,
                                     String[] dataStoreServiceDescription,
                                     DataStoreServiceRegistrar registrar,
                                     StatisticsProvider statisticsProvider,
                                     boolean useJR2Caching) throws RepositoryException {
        Properties properties = new Properties();
        properties.putAll(config);

        AzureDataStore dataStore = new AzureDataStore();
        dataStore.setStatisticsProvider(statisticsProvider);
        dataStore.setProperties(properties);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, dataStoreServiceDescription);

        if (null != registrar) {
            registrar.setClassNames(new String[] {
                    AbstractSharedCachingDataStore.class.getName(),
                    AbstractSharedCachingDataStore.class.getName()
            });
            registrar.setConfig(props);
        }

        return dataStore;
    }
}
