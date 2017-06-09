package org.apache.jackrabbit.oak.blob.federated;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.osgi.framework.*;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class AbstractFederatedDataStoreService extends AbstractDataStoreService
    implements BundleListener, FrameworkListener {
    private static Logger LOG = LoggerFactory.getLogger(AbstractFederatedDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";
    private static final String DATASTORE_PRIMARY = "datastore.primary";
    private static final String DATASTORE_SECONDARY = "datastore.secondary";

    private ServiceRegistration delegateReg;

    private DataStore defaultDelegateDataStore = null;
    private DataStoreSpecification defaultDelegateDataStoreSpec = null;
    private Map<String, DataStore> secondaryDelegateDataStores = Maps.newConcurrentMap();
    private Multimap<String, DataStoreSpecification> secondaryDelegateDataStoreSpecsByName = HashMultimap.create();
    private Multimap<String, DataStoreSpecification> secondaryDelegateDataStoreSpecsByBundle = HashMultimap.create();

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        Properties properties = new Properties();

        // Parse config to get a list of all the data stores we want to use.
        for (Map.Entry<String, Object> entry: config.entrySet()) {
            if (DATASTORE_PRIMARY.equals(entry.getKey())) {
                if (null != defaultDelegateDataStoreSpec) {
                    LOG.warn("Configuration error - Duplicate specification of primary data store");
                }
                else {
                    defaultDelegateDataStoreSpec = DataStoreSpecification.create((String) entry.getValue());
                }
            }
            else if (DATASTORE_SECONDARY.equals(entry.getKey())){
                DataStoreSpecification spec = DataStoreSpecification.create((String) entry.getValue());
                if (null != spec) {
                    secondaryDelegateDataStoreSpecsByName.put(spec.dataStoreName, spec);
                    secondaryDelegateDataStoreSpecsByBundle.put(spec.bundleName, spec);
                }
            }
            else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        // Register a bundle listener to create data stores for any specified in config whose bundles
        // will be activated later.
        // This listener will also shut down any active data stores if their corresponding bundle is
        // shut down.
        context.getBundleContext().addBundleListener(this);
        // This listener will also log any data stores that didn't get set up by the time the framework is started.
        context.getBundleContext().addFrameworkListener(this);

        // Get a list of all the bundles now, and use the bundles to create any data stores pertaining
        // to bundles that are already active.
        for (Bundle bundle : context.getBundleContext().getBundles()) {
            addDataStoresFromBundle(bundle);
        }

        if (null == defaultDelegateDataStore) {
            LOG.error("Configuration error - FederatedDataStore cannot determine the default delegate data store");
        }
        //FederatedDataStore dataStore = new FederatedDataStore(defaultDelegateDataStore, secondaryDelegateDataStores);
        //dataStore.setProperties(properties);

        //Dictionary<String, Object> props = new Hashtable<String, Object>();
        //props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        //props.put(DESCRIPTION, getDescription());

        //delegateReg = context.getBundleContext().registerService(new String[] {
        //        DataStore.class.getName(),
        //        DataStore.class.getName()
        //}, dataStore, props);

        //return dataStore;
        DataStore ds = new FileDataStore();
        return ds;
    }

    /**
     * Using a bundle, determine if that bundle is one used by
     * one or more data stores specified in the configuration,
     * and if so, attempt to load the data store(s) from that bundle.
     * @param bundle
     */
    private void addDataStoresFromBundle(final Bundle bundle) {
        String bundleName = bundle.getSymbolicName();
        LOG.info("FDSS - Checking bundle {} for data stores", bundle.getSymbolicName());
        if (defaultDelegateDataStoreSpec.bundleName.equals(bundleName)) {
            LOG.info("FDSS - Found bundle {} for default data store", bundle.getSymbolicName());
            if (null != defaultDelegateDataStore) {
                LOG.warn("Configuration error - Attempted to create additional primary data store");
            } else {
                defaultDelegateDataStore = createDataStoreFromBundle(defaultDelegateDataStoreSpec, bundle);
            }
        }
        for (DataStoreSpecification spec : secondaryDelegateDataStoreSpecsByBundle.get(bundleName)) {
            if (null != spec) {
                LOG.info("FDSS - Found bundle {} for secondary data store", bundle.getSymbolicName());
                DataStore ds = createDataStoreFromBundle(spec, bundle);
                if (null != ds) {
                    secondaryDelegateDataStores.put(spec.dataStoreName, ds);
                }
            }
        }
    }

    private DataStore createDataStoreFromBundle(final DataStoreSpecification spec, final Bundle bundle) {
        LOG.info("FDSS - Trying to create data store with spec {} and bundle {}", spec, bundle.getSymbolicName());
        if (bundle.getState() == Bundle.ACTIVE) {
            try {
                LOG.info("FDSS - Getting new class instance for {} from bundle {}", spec.packageName, bundle.getSymbolicName());
                return (DataStore) bundle.loadClass(spec.packageName).newInstance();
            } catch (InstantiationException e) {
                LOG.warn(String.format("InstantiationException while trying to create data store %s", spec.dataStoreName), e);
            } catch (IllegalAccessException e) {
                LOG.warn(String.format("IllegalAccessException while trying to create data store %s", spec.dataStoreName), e);
            } catch (ClassNotFoundException e) {
                LOG.warn(String.format("No class found for data store %s", spec.dataStoreName), e);
            }
        }
        return null;
    }

    @Override
    public void bundleChanged(BundleEvent event) {
        if (event.getType() == BundleEvent.STARTED) {
            LOG.info("FDSS - Bundle {} started, checking for data stores", event.getBundle().getSymbolicName());
            addDataStoresFromBundle(event.getBundle());
        }
        else if (event.getType() == BundleEvent.STOPPING) {
            String bundleName = event.getBundle().getSymbolicName();
            if (bundleName.equals(defaultDelegateDataStoreSpec.bundleName)) {
                try {
                    defaultDelegateDataStore.close();
                } catch (DataStoreException e) {
                    e.printStackTrace();
                }
                defaultDelegateDataStore = null;
            }
            for (DataStoreSpecification spec : secondaryDelegateDataStoreSpecsByBundle.get(bundleName)) {
                DataStore toRemove = secondaryDelegateDataStores.remove(spec.dataStoreName);
                if (null != toRemove) {
                    try {
                        toRemove.close();
                    } catch (DataStoreException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void frameworkEvent(FrameworkEvent event) {
        if (event.getType() == FrameworkEvent.STARTED) {
            // Check for bundles
        }
    }

    private enum DataStoreSpecification {
        FILE_DATA_STORE (
                "FileDataStore",
                "org.apache.jackrabbit.oak.plugins.blob.datastore.OakCachingFDS",
                "org.apache.jackrabbit.oak-core"
        ),
        S3_DATA_STORE (
                "S3DataStore",
                "org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore",
                "org.apache.jackrabbit.oak-blob-cloud"
        ),
        AZURE_DATA_STORE (
                "AzureDataStore",
                "org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore",
                "org.apache.jackrabbit.oak-blob-cloud-azure"
        );

        private final String dataStoreName;
        private final String packageName;
        private final String bundleName;

        private Properties properties = new Properties();

        DataStoreSpecification(final String dataStoreName,
                               final String packageName,
                               final String bundleName) {
            this.dataStoreName = dataStoreName;
            this.packageName = packageName;
            this.bundleName = bundleName;
        }

        @Override
        public String toString() {
            return dataStoreName;
        }

        private DataStoreSpecification withConfig(final Map<String, Object> cfg) {
            for (Map.Entry<String, Object> entry : cfg.entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        static DataStoreSpecification create(final String dataStoreConfig) {
            Map<String, Object> cfgMap = Maps.newHashMap();
            List<String> cfgPairs = Lists.newArrayList(dataStoreConfig.split(","));
            String dataStoreName = null;
            for (String s : cfgPairs) {
                String[] parts = s.split(":");
                if (parts.length != 2) continue;
                if ("name".equals(parts[0])) {
                    dataStoreName = parts[1];
                }
                else {
                    cfgMap.put(parts[0], parts[1]);
                }
            }

            if (DataStoreSpecification.FILE_DATA_STORE.dataStoreName.equals(dataStoreName)) {
                return DataStoreSpecification.FILE_DATA_STORE.withConfig(cfgMap);
            }
            else if (DataStoreSpecification.S3_DATA_STORE.dataStoreName.equals(dataStoreName)) {
                return DataStoreSpecification.S3_DATA_STORE.withConfig(cfgMap);
            }
            else if (DataStoreSpecification.AZURE_DATA_STORE.dataStoreName.equals(dataStoreName)) {
                return DataStoreSpecification.AZURE_DATA_STORE.withConfig(cfgMap);
            }
            return null;
        }
    }

    protected void deactivate() throws DataStoreException {
        // shut down all delegate data stores
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            ds.close();
        }
        defaultDelegateDataStore.close();

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
