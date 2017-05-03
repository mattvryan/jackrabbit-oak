package org.apache.jackrabbit.oak.blob.federated;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.ConfigurableDataStore;

import javax.jcr.RepositoryException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class FederatedDataStore implements ConfigurableDataStore {
    protected Properties properties;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    private ConfigurableDataStore defaultDS;
    private Map<String, ConfigurableDataStore> delegateDataStores = Maps.newConcurrentMap();

    public Map<String, ConfigurableDataStore> getDelegateDataStores() {
        return this.delegateDataStores;
    }

    public FederatedDataStore(final ConfigurableDataStore defaultDS, final Map<String, ConfigurableDataStore> delegateDataStores) {
        this.defaultDS = defaultDS;
        this.delegateDataStores = delegateDataStores;
    }


    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        return defaultDS.getRecordIfStored(identifier);
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        return defaultDS.getRecord(identifier);
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        return defaultDS.getRecordFromReference(reference);
    }

    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        return defaultDS.addRecord(stream);
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        defaultDS.updateModifiedDateOnAccess(before);
    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        return defaultDS.deleteAllOlderThan(min);
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return defaultDS.getAllIdentifiers();
    }

    @Override
    public void init(String homeDir) throws RepositoryException {
        defaultDS.init(homeDir);
        for (ConfigurableDataStore ds : delegateDataStores.values()) {
            ds.init(homeDir);
        }
    }

    @Override
    public int getMinRecordLength() {
        return defaultDS.getMinRecordLength();
    }

    @Override
    public void close() throws DataStoreException {
        defaultDS.close();
        for (ConfigurableDataStore ds : delegateDataStores.values()) {
            ds.close();
        }
    }

    @Override
    public void clearInUse() {
        defaultDS.clearInUse();
        for (ConfigurableDataStore ds : delegateDataStores.values()) {
            ds.clearInUse();
        }
    }
}
