package org.apache.jackrabbit.oak.blob.federated;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractFederatedDataStoreService extends AbstractDataStoreService {
    private static Logger LOG = LoggerFactory.getLogger(AbstractFederatedDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";

    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        Properties properties = new Properties();
        properties.putAll(config);
        LOG.warn("CreateDataStore() in AbstractFederatedDataStoreService.class");
        for (Map.Entry<String, Object> e : config.entrySet()) {
            LOG.warn(String.format("Config entry: %s=\"%s\"", e.getKey(), e.getValue().toString()));
        }

        FederatedDataStore dataStore = new FederatedDataStore();
        //dataStore.setStatisticsProvider(getStatisticsProvider());
        dataStore.setProperties(properties);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());

        //delegateReg = context.getBundleContext().registerService(new String[] {
        //        AbstractSharedCachingDataStore.class.getName(),
        //        AbstractSharedCachingDataStore.class.getName()
        //}, dataStore , props);
        delegateReg = context.getBundleContext().registerService(new String[] {
                DataStore.class.getName(),
                DataStore.class.getName()
        }, dataStore, props);

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
        return new String[] {"type=FederatedBlob"};
    }
}
