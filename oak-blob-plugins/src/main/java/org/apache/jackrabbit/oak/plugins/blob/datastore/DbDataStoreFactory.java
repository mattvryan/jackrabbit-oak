package org.apache.jackrabbit.oak.plugins.blob.datastore;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.db.DbDataStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.service.component.ComponentContext;

import javax.jcr.RepositoryException;
import java.util.Map;

public class DbDataStoreFactory implements DataStoreFactory {
    @Override
    public DataStore createDataStore(final ComponentContext context,
                                     final Map<String, Object> config,
                                     final String[] dataStoreServiceDescription,
                                     final DataStoreServiceRegistrar registrar,
                                     final StatisticsProvider statisticsProvider,
                                     final boolean useJR2Caching)
            throws RepositoryException {
        return new DbDataStore();
    }
}
