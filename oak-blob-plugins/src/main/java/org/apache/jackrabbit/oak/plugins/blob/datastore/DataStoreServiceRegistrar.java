package org.apache.jackrabbit.oak.plugins.blob.datastore;

import org.apache.jackrabbit.core.data.DataStore;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import java.util.Dictionary;

public class DataStoreServiceRegistrar {
    private String[] classNames;
    private Dictionary<String, Object> config;
    private BundleContext bundleContext;

    public DataStoreServiceRegistrar(final BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    public void setClassNames(final String[] classNames) {
        this.classNames = classNames;
    }

    public void setConfig(final Dictionary<String, Object> config) {
        this.config = config;
    }

    public boolean needsDelegateRegistration() {
        return null != classNames && classNames.length > 0 && null != config && null != bundleContext;
    }

    public ServiceRegistration createDelegateRegistration(final DataStore dataStore) {
        ServiceRegistration reg = null;
        if (needsDelegateRegistration()) {
            reg = bundleContext.registerService(classNames, dataStore, config);
        }
        return reg;
    }
}
