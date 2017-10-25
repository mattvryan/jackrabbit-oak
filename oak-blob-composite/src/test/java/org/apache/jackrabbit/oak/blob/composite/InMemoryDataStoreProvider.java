package org.apache.jackrabbit.oak.blob.composite;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.InMemoryDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;

public class InMemoryDataStoreProvider extends InMemoryDataStore implements DataStoreProvider {
    @Override
    public DataStore getDataStore() {
        return this;
    }
}
