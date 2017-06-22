package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.felix.scr.annotations.Component;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Component(componentAbstract = true)
public abstract class AbstractCompositeDataStoreService extends AbstractDataStoreService {
    private static Logger LOG = LoggerFactory.getLogger(AbstractCompositeDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";
    private static final String DATASTORE_PRIMARY = "datastore.primary";
    private static final String DATASTORE_SECONDARY = "datastore.secondary";

    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        Properties properties = new Properties();

        CompositeDataStore dataStore = new CompositeDataStore();

        // Register a bundle listener to create data stores for any specified in config whose bundles
        // will be activated later.
        // This listener will also shut down any active data stores if their corresponding bundle is
        // shut down.
        context.getBundleContext().addBundleListener(dataStore);
        // This listener will also log any data stores that didn't get set up by the time the framework is started.
        context.getBundleContext().addFrameworkListener(dataStore);

        // Parse config to get a list of all the data stores we want to use.
        for (Map.Entry<String, Object> entry: config.entrySet()) {
            if (DATASTORE_PRIMARY.equals(entry.getKey())) {
                Properties dsProps = parseProperties((String)entry.getValue());
                if (null == dsProps.get("path")) { dsProps.put("path", "/repository/datastore/primary"); }
                dsProps.put("homeDir", lookup(context, "repository.home"));
                dataStore.setDefaultDataStoreSpecification(dsProps);
            }
            else if (DATASTORE_SECONDARY.equals(entry.getKey())){
                Properties dsProps = parseProperties((String)entry.getValue());
                if (null == dsProps.get("path")) { dsProps.put("path", "/repostory/datastore/secondary"); }
                dsProps.put("homeDir", lookup(context, "repository.home"));
                dataStore.addSecondaryDataStoreSpecification(dsProps);
            }
            else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        dataStore.setProperties(properties);

        dataStore.addActiveDataStores(context.getBundleContext());

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());

        delegateReg = context.getBundleContext().registerService(new String[] {
                DataStore.class.getName(),
                CompositeDataStore.class.getName()
        }, dataStore, props);

        return dataStore;
    }

    private Properties parseProperties(final String props) {
        Map<String, Object> cfgMap = Maps.newHashMap();
        List<String> cfgPairs = Lists.newArrayList(props.split(","));
        for (String s : cfgPairs) {
            String[] parts = s.split(":");
            if (parts.length != 2) continue;
            cfgMap.put(parts[0], parts[1]);
        }
        Properties properties = new Properties();
        properties.putAll(cfgMap);
        return properties;
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
