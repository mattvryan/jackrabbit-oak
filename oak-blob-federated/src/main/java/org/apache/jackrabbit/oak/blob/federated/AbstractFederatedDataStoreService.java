package org.apache.jackrabbit.oak.blob.federated;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public abstract class AbstractFederatedDataStoreService extends AbstractDataStoreService {
    private static Logger LOG = LoggerFactory.getLogger(AbstractFederatedDataStoreService.class);

    private static final String DESCRIPTION = "oak.datastore.description";
    private static final String DATASTORE_PRIMARY = "datastore.primary";
    private static final String DATASTORE_SECONDARY = "datastore.secondary";

    private ServiceRegistration delegateReg;

    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        Properties properties = new Properties();

        DataStore defaultDelegateDS = null;
        Map<String, DataStore> dataStoreDelegates = Maps.newConcurrentMap();

        for (Map.Entry<String, Object> entry: config.entrySet()) {
            if (DATASTORE_PRIMARY.equals(entry.getKey())) {
                defaultDelegateDS = getDelegateDataStore((String) entry.getValue());
            }
            else if (DATASTORE_SECONDARY.equals(entry.getKey())){
                DataStore delegate = getDelegateDataStore((String) entry.getValue());
                if (null != delegate) {
                    dataStoreDelegates.put((String) entry.getValue(), delegate);
                }
            }
            else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        if (null == defaultDelegateDS) {
            LOG.error("Configuration error - FederatedDataStore cannot determine the default delegate data store");
        }
        FederatedDataStore dataStore = new FederatedDataStore(defaultDelegateDS, dataStoreDelegates);
        dataStore.setProperties(properties);

        Dictionary<String, Object> props = new Hashtable<String, Object>();
        props.put(Constants.SERVICE_PID, dataStore.getClass().getName());
        props.put(DESCRIPTION, getDescription());

        delegateReg = context.getBundleContext().registerService(new String[] {
                DataStore.class.getName(),
                DataStore.class.getName()
        }, dataStore, props);

        return dataStore;
    }

    private DataStore getDelegateDataStore(final String dsConfig) {
        Map<String, Object> cfg = Maps.newHashMap();
        List<String> cfgPairs = Lists.newArrayList(dsConfig.split(","));
        String name = null;
        for (String s : cfgPairs) {
            String[] parts = s.split(":");
            if (parts.length != 2) continue;
            if ("name".equals(parts[0])) {
                name = parts[1];
            }
            else {
                cfg.put(parts[0], parts[1]);
            }
        }
        String fullClassName = null;
        DataStore ds = null;
        if (null != name) {
            switch (name) {
                case "FileDataStore":
                    fullClassName = "org.apache.jackrabbit.oak.plugins.blob.datastore.OakCachingFDS";
                    break;
                case "S3DataStore":
                    fullClassName = "org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore";
                    break;
                default:
                    break;
            }
            if (null != fullClassName) {
                try {
                    Class dataStoreClass = Class.forName(fullClassName);
                    ds = (DataStore) dataStoreClass.newInstance();
                    try {
                        Method setPropertiesMethod = dataStoreClass.getMethod("setProperties", Properties.class);
                        Properties properties = new Properties();
                        properties.putAll(cfg);
                        setPropertiesMethod.invoke(ds, properties);
                    }
                    catch (InvocationTargetException ite) {
                        LOG.warn("FEDERATED-DATA-STORE - Could not set properties for data store class {}", fullClassName);
                    }
                    catch (NoSuchMethodException nsme) { } // Don't worry about this
                } catch (ClassNotFoundException cnfe) {
                    LOG.warn("FEDERATED-DATA-STORE - Could not get class object for data store class {}", fullClassName);
                } catch (InstantiationException ie) {
                    LOG.warn("FEDERATED-DATA-STORE - Could not instantiate data store class {}", fullClassName);
                } catch (IllegalAccessException iae) {
                    LOG.warn("FEDERATED-DATA-STORE - illegal access trying to instantiate data store class {}", fullClassName);
                }
            }
        }
        return ds;
    }

    protected void deactivate() throws DataStoreException {
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
