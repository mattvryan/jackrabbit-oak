package org.apache.jackrabbit.oak.plugins.blob.datastore;

import org.apache.jackrabbit.core.data.DataStore;

import java.util.Properties;

public interface ConfigurableDataStore extends DataStore {
    void setProperties(final Properties properties);
}
