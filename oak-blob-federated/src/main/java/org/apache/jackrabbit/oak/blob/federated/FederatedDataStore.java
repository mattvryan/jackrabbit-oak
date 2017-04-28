package org.apache.jackrabbit.oak.blob.federated;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;

import javax.jcr.RepositoryException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;

public class FederatedDataStore implements DataStore {
    protected Properties properties;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }


    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        return null;
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        return null;
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        return null;
    }

    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        return null;
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {

    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        return 0;
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return null;
    }

    @Override
    public void init(String homeDir) throws RepositoryException {

    }

    @Override
    public int getMinRecordLength() {
        return 0;
    }

    @Override
    public void close() throws DataStoreException {

    }

    @Override
    public void clearInUse() {

    }
}
