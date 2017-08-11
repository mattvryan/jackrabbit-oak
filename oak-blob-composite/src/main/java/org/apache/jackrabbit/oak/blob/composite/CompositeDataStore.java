/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.osgi.framework.*;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookup;

public class CompositeDataStore implements DataStore, BundleListener, FrameworkListener {
    private static final String DATASTORE = "datastore";

    private static Logger LOG = LoggerFactory.getLogger(CompositeDataStore.class);

    protected Properties properties = new Properties();
    private Map<String, DelegateDataStoreSpec> dataStoreSpecsByBundleName = Maps.newConcurrentMap();

    private Set<Bundle> bundlesWithActiveDataStores = Sets.newHashSet();

    private DelegateTraversalStrategy traversalStrategy = new IntelligentDelegateTraversalStrategy();

    public void setProperties(final Map<String, Object> config, final ComponentContext context) {
        // Parse config to get a list of all the data stores we want to use.
        Map<String, DelegateDataStoreSpec> specs = Maps.newConcurrentMap();
        for (Map.Entry<String, Object> entry: config.entrySet()) {
            if (DATASTORE.equals(entry.getKey())) {
                Properties dsProps = parseProperties((String)entry.getValue());
                dsProps.put("homeDir", lookup(context, "repository.home"));
                Optional<DelegateDataStoreSpec> spec = DelegateDataStoreSpec.createFromProperties(dsProps);
                if (spec.isPresent()) {
                    specs.put(spec.get().getBundleName(), spec.get());
                }
            }
            else {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

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

    void addActiveDataStores(final BundleContext context) {
        // Get a list of all the bundles now, and use the bundles to create any data stores pertaining
        // to bundles that are already active.
        for (Bundle bundle : context.getBundles()) {
            addDataStoresFromBundle(bundle);
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
        if (dataStoreSpecsByBundleName.containsKey(bundleName)) {
            DelegateDataStoreSpec spec = dataStoreSpecsByBundleName.get(bundleName);
            DataStore ds = createDataStoreFromBundle(spec, bundle);
            if (null != spec && null != ds) {
                traversalStrategy.addDelegateDataStore(ds, spec);
                bundlesWithActiveDataStores.add(bundle);
            }
        }
    }

    private DataStore createDataStoreFromBundle(final DelegateDataStoreSpec spec, final Bundle bundle) {
        LOG.info("FDSS - Trying to create data store with spec {} and bundle {}", spec, bundle.getSymbolicName());
        if (bundle.getState() == Bundle.ACTIVE) {
            try {
                LOG.info("FDSS - Getting new class instance for {} from bundle {}", spec.getClassName(), bundle.getSymbolicName());
                Class dsClass = bundle.loadClass(spec.getClassName());
                DataStore ds = (DataStore) dsClass.newInstance();
                try {
                    Method setPropertiesMethod = dsClass.getMethod("setProperties", Properties.class);
                    setPropertiesMethod.invoke(ds, spec.getProperties());
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
        DataRecord result = null;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(identifier);
        while (iter.hasNext()) {
            result = iter.next().getRecordIfStored(identifier);
            if (null != result) {
                break;
            }
        }
        return result;
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        DataRecord result = null;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(identifier);
        while (iter.hasNext()) {
            result = iter.next().getRecord(identifier);
            if (null != result) {
                break;
            }
        }
        return result;
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        return getRecord(getIdentifierFromReference(reference));
    }

    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        DataStore dataStore = traversalStrategy.selectWritableDelegate(identifier);
        return dataStore.addRecord(stream);
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            iter.next().updateModifiedDateOnAccess(before);;
        }
    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        // Attempt to delete all records in all writable delegates older than min.
        // First thrown data store exception kills the whole thing.
        Iterator<DataStore> iter = traversalStrategy.getWritableDelegateIterator();
        int nDeleted = 0;
        while (iter.hasNext()) {
            nDeleted += iter.next().deleteAllOlderThan(min);
        }
        return nDeleted;
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        // Attempt to iterate through all identifiers in all delegates.
        // First thrown data store exception kills the whole thing.
        Iterator<DataIdentifier> result = null;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            if (null == result) {
                result = iter.next().getAllIdentifiers();
            }
            else {
                result = Iterators.concat(result, iter.next().getAllIdentifiers());
            }
        }
        return result;
    }

    @Override
    public void init(String homeDir) throws RepositoryException {
        // Attempt to initialize all delegates.
        // First thrown repository exception kills the whole thing.
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            iter.next().init(homeDir);
        }
    }

    @Override
    public int getMinRecordLength() {
        int minRecordLength = -1;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            if (-1 == minRecordLength) {
                minRecordLength = iter.next().getMinRecordLength();
            }
            else {
                int smin = iter.next().getMinRecordLength();
                if (smin < minRecordLength) {
                    minRecordLength = smin;
                }
            }
        }
        return minRecordLength;
    }

    @Override
    public void close() throws DataStoreException {
        // Attempt to close all delegates.
        // If an exception is thrown, catch it and continue trying to
        // close the remaining delegates.
        // Rethrow the first caught exception if there is one.
        List<DataStoreException> exceptionsCaught = Lists.newArrayList();
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            try {
                iter.next().close();
            }
            catch (DataStoreException dse) {
                exceptionsCaught.add(dse);
            }
        }
        if (! exceptionsCaught.isEmpty()) {
            throw new DataStoreException(String.format("CompositeDataStore.close() caught %d close exceptions.  First exception:",
                    exceptionsCaught.size()),
                    exceptionsCaught.get(0));
        }
    }

    @Override
    public void clearInUse() {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            iter.next().clearInUse();
        }
    }

    @Override
    public void bundleChanged(BundleEvent event) {
        if (event.getType() == BundleEvent.STARTED) {
            LOG.info("FDSS - Bundle {} started, checking for data stores", event.getBundle().getSymbolicName());
            addDataStoresFromBundle(event.getBundle());
        }
        else if (event.getType() == BundleEvent.STOPPING) {
            traversalStrategy.removeDelegateDataStoresForBundle(event.getBundle());
        }
    }

    @Override
    public void frameworkEvent(FrameworkEvent event) {
        if (event.getType() == FrameworkEvent.STARTED && ! traversalStrategy.hasDelegate()) {
            LOG.error("Framework started, but no delegate data stores were started");
        }
    }
}
