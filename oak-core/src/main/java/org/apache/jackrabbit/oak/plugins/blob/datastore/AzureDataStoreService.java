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
