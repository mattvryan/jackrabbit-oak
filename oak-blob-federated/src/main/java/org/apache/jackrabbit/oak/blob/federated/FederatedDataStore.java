package org.apache.jackrabbit.oak.blob.federated;

import com.google.common.collect.*;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class FederatedDataStore implements DataStore, BundleListener, FrameworkListener {
    private static Logger LOG = LoggerFactory.getLogger(FederatedDataStore.class);
    protected Properties properties;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    private Map<DataIdentifier, DataStore> dsMap = Maps.newConcurrentMap();

    private Set<String> bundlesWithActiveDataStores = Sets.newHashSet();

    private DataStoreSpecification defaultDelegateDataStoreSpec = null;
    private Multimap<String, DataStoreSpecification> secondaryDelegateDataStoreSpecsByName = HashMultimap.create();
    private Multimap<String, DataStoreSpecification> secondaryDelegateDataStoreSpecsByBundle = HashMultimap.create();

    private DataStore defaultDelegateDataStore = null;
    private Map<String, DataStore> secondaryDelegateDataStores = Maps.newConcurrentMap();

    public Map<String, DataStore> getSecondaryDelegateDataStores() {
        return this.secondaryDelegateDataStores;
    }

    void setDefaultDataStoreSpecification(final Properties properties) {
        this.defaultDelegateDataStoreSpec = DataStoreSpecification.create(properties);
    }

    void addSecondaryDataStoreSpecification(final Properties properties) {
        DataStoreSpecification spec = DataStoreSpecification.create(properties);
        secondaryDelegateDataStoreSpecsByName.put(spec.dataStoreName, spec);
        secondaryDelegateDataStoreSpecsByBundle.put(spec.bundleName, spec);
    }

    void addActiveDataStores(BundleContext context) {
        // Get a list of all the bundles now, and use the bundles to create any data stores pertaining
        // to bundles that are already active.
        for (Bundle bundle : context.getBundles()) {
            addDataStoresFromBundle(bundle);
        }

        if (null == defaultDelegateDataStore) {
            LOG.error("Configuration error - FederatedDataStore cannot determine the default delegate data store");
        }
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
                bundlesWithActiveDataStores.add(bundle.getSymbolicName());
            }
        }
        for (DataStoreSpecification spec : secondaryDelegateDataStoreSpecsByBundle.get(bundleName)) {
            if (null != spec) {
                LOG.info("FDSS - Found bundle {} for secondary data store", bundle.getSymbolicName());
                DataStore ds = createDataStoreFromBundle(spec, bundle);
                if (null != ds) {
                    secondaryDelegateDataStores.put(spec.dataStoreName, ds);
                    bundlesWithActiveDataStores.add(bundle.getSymbolicName());
                }
            }
        }
    }

    private DataStore createDataStoreFromBundle(final DataStoreSpecification spec, final Bundle bundle) {
        LOG.info("FDSS - Trying to create data store with spec {} and bundle {}", spec, bundle.getSymbolicName());
        if (bundle.getState() == Bundle.ACTIVE) {
            try {
                LOG.info("FDSS - Getting new class instance for {} from bundle {}", spec.packageName, bundle.getSymbolicName());
                Class dsClass = bundle.loadClass(spec.packageName);
                DataStore ds = (DataStore) dsClass.newInstance();
                try {
                    Method setPropertiesMethod = dsClass.getMethod("setProperties", Properties.class);
                    setPropertiesMethod.invoke(ds, spec.properties);
                    if (ds.getClass().getSimpleName().equals("OakCachingFDS")) {
                        // TODO:  This all needs to be fixed.  We need a way to
                        // delegate property construction of a data store to the
                        // data store itself, not to the service class. -MR
                        String cs = spec.properties.getProperty("cacheSize", null);
                        long cacheSize = null == cs ? 0L : Long.parseLong(cs);
                        String fsBackendPath = spec.properties.getProperty("path", null);
                        fsBackendPath = null==fsBackendPath ? spec.properties.getProperty("homeDir", null) :
                                spec.properties.getProperty("homeDir") + fsBackendPath;
                        spec.properties.remove("path");
                        spec.properties.put("fsBackendPath", fsBackendPath);
                        spec.properties.put("cacheSize", cacheSize);
                        String cachePath = spec.properties.getProperty("cachepath", null);
                        if (null != cachePath) {
                            spec.properties.remove("cachePath");
                            spec.properties.put("path", cachePath);
                        }
                    }
                }
                catch (NoSuchMethodException | InvocationTargetException e) { }
                return ds;
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

    private DataIdentifier getIdentifierFromReference(String reference) {
        if (reference != null) {
            int colon = reference.indexOf(':');
            if (colon != -1) {
                return new DataIdentifier(reference.substring(0, colon));
            }
        }
        return null;
    }

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        //return defaultDelegateDataStore.getRecordIfStored(identifier);
        return dsMap.get(identifier).getRecordIfStored(identifier);
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        //return defaultDelegateDataStore.getRecord(identifier);
        return dsMap.get(identifier).getRecord(identifier);
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        //return defaultDelegateDataStore.getRecordFromReference(reference);
        return dsMap.get(getIdentifierFromReference(reference)).getRecordFromReference(reference);
    }

    private static final Object lock = new Object();
    private static boolean useDefault = true;
    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        DataStore ds;
        synchronized(lock) {
            ds = useDefault ? defaultDelegateDataStore : secondaryDelegateDataStores.values().iterator().next();
            useDefault = !useDefault;
        }
        DataRecord record = ds.addRecord(stream);
        dsMap.put(record.getIdentifier(), ds);
        return record;
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        defaultDelegateDataStore.updateModifiedDateOnAccess(before);
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            ds.updateModifiedDateOnAccess(before);
        }
    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        int nDeleted = defaultDelegateDataStore.deleteAllOlderThan(min);

        for (DataStore ds : secondaryDelegateDataStores.values()) {
            nDeleted += ds.deleteAllOlderThan(min);
        }

        return nDeleted;
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        //return defaultDelegateDataStore.getAllIdentifiers();
        Iterator<DataIdentifier> iter = defaultDelegateDataStore.getAllIdentifiers();
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            iter = Iterators.concat(iter, ds.getAllIdentifiers());
        }
        return iter;
    }

    @Override
    public void init(String homeDir) throws RepositoryException {
        defaultDelegateDataStore.init(homeDir);
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            ds.init(homeDir);
        }
    }

    @Override
    public int getMinRecordLength() {
        int minRecordLength = defaultDelegateDataStore.getMinRecordLength();
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            int smin = ds.getMinRecordLength();
            if (smin < minRecordLength) {
                minRecordLength = smin;
            }
        }
        return minRecordLength;
    }

    @Override
    public void close() throws DataStoreException {
        defaultDelegateDataStore.close();
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            ds.close();
        }
    }

    @Override
    public void clearInUse() {
        defaultDelegateDataStore.clearInUse();
        for (DataStore ds : secondaryDelegateDataStores.values()) {
            ds.clearInUse();
        }
    }

    @Override
    public void bundleChanged(BundleEvent event) {
        if (event.getType() == BundleEvent.STARTED) {
            LOG.info("FDSS - Bundle {} started, checking for data stores", event.getBundle().getSymbolicName());
            addDataStoresFromBundle(event.getBundle());
        }
        else if (event.getType() == BundleEvent.STOPPING) {
            String bundleName = event.getBundle().getSymbolicName();
            if (bundlesWithActiveDataStores.contains(bundleName)) {
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
    }

    @Override
    public void frameworkEvent(FrameworkEvent event) {
        if (event.getType() == FrameworkEvent.STARTED) {
            // Check for bundles
            if (bundlesWithActiveDataStores.size() != secondaryDelegateDataStores.size() + 1) {
                if (null == defaultDelegateDataStore) {
                    LOG.error("Framework started, but no default delegate data store was started");
                }
                else {
                    for (String bundleName : secondaryDelegateDataStoreSpecsByBundle.keySet()) {
                        if (! bundlesWithActiveDataStores.contains(bundleName)) {
                            LOG.warn("Framework started, but secondary data store {} was not started", bundleName);
                        }
                    }
                }
            }
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

        private DataStoreSpecification withProperties(final Properties properties) {
            this.properties.putAll(properties);
            return this;
        }

        static DataStoreSpecification create(final Properties properties) {
            final String dataStoreName = properties.getProperty("dataStoreName");
            if (null == dataStoreName) {
                LOG.warn("No 'dataStoreName' property specified for data store in config; cannot create data store");
            }
            else {
                if (DataStoreSpecification.FILE_DATA_STORE.dataStoreName.equals(dataStoreName)) {
                    return DataStoreSpecification.FILE_DATA_STORE.withProperties(properties);
                } else if (DataStoreSpecification.S3_DATA_STORE.dataStoreName.equals(dataStoreName)) {
                    return DataStoreSpecification.S3_DATA_STORE.withProperties(properties);
                } else if (DataStoreSpecification.AZURE_DATA_STORE.dataStoreName.equals(dataStoreName)) {
                    return DataStoreSpecification.AZURE_DATA_STORE.withProperties(properties);
                }
            }
            return null;
        }
    }
}
