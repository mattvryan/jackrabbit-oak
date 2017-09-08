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
import org.apache.commons.io.FilenameUtils;
import org.apache.jackrabbit.core.data.*;
import org.apache.jackrabbit.oak.plugins.blob.CompositeDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.DataIdentifierCreationResult;
import org.apache.jackrabbit.oak.plugins.blob.DataIdentifierFactory;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.osgi.framework.*;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookup;

public class CompositeDataStore implements DataStore, SharedDataStore, TypedDataStore, MultiDataStoreAware, BundleListener, FrameworkListener {
    private static final String DATASTORE = "datastore";

    private static Logger LOG = LoggerFactory.getLogger(CompositeDataStore.class);

    protected Properties properties = new Properties();
    private Map<String, List<DelegateDataStoreSpec>> dataStoreSpecsByBundleName = Maps.newConcurrentMap();

    private Set<Bundle> bundlesWithActiveDataStores = Sets.newHashSet();

    private DelegateTraversal traversalStrategy = new IntelligentDelegateTraversal();

    private String path;
    private File rootDirectory;
    private File tmp;

    public void setProperties(final Map<String, Object> config, final ComponentContext context) {
        // Parse config to get a list of all the data stores we want to use.
        for (Map.Entry<String, Object> entry: config.entrySet()) {
            LOG.debug("Read configuration entry {}:{}", entry.getKey(), entry.getValue());
            if (entry.getKey().startsWith(DATASTORE)) {
                LOG.debug("{} is a delegate datastore definition", entry.getKey());
                Properties dsProps = parseProperties((String)entry.getValue());
                if (! dsProps.containsKey("homeDir")) {
                    String homeDir = lookup(context, "repository.home");
                    if (null != homeDir) {
                        dsProps.put("homeDir", homeDir);
                    }
                    else {
                        homeDir = lookup(context, "path");
                        if (null != homeDir) {
                            dsProps.put("homeDir", homeDir);
                        }
                    }
                }
                Optional<DelegateDataStoreSpec> specOpt = DelegateDataStoreSpec.createFromProperties(dsProps);
                if (specOpt.isPresent()) {
                    DelegateDataStoreSpec spec = specOpt.get();
                    LOG.debug("Generated spec '{}' from configuration '{}'", spec.toString(), entry.getValue());
                    if (! dataStoreSpecsByBundleName.containsKey(spec.getBundleName())) {
                        dataStoreSpecsByBundleName.put(spec.getBundleName(), Lists.newArrayList());
                    }
                    dataStoreSpecsByBundleName.get(spec.getBundleName()).add(spec);
                }
                else {
                    LOG.debug("Could not generate spec from configuration '{}'", entry.getValue());
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
        if (! properties.containsKey("homeDir")) {
            if (properties.containsKey("repository.home")) {
                properties.put("homeDir", properties.getProperty("repository.home"));
            } else if (properties.containsKey("path")) {
                properties.put("homeDir", properties.getProperty("path"));
            }
        }
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
        if (dataStoreSpecsByBundleName.containsKey(bundleName)) {
            List<DelegateDataStoreSpec> specs = dataStoreSpecsByBundleName.get(bundleName);
            for (DelegateDataStoreSpec spec : specs) {
                DataStore ds = createDataStoreFromBundle(spec, bundle);
                if (null != spec && null != ds) {
                    traversalStrategy.addDelegateDataStore(ds, spec);
                    bundlesWithActiveDataStores.add(bundle);
                }
            }
        }
    }

    private DataStore createDataStoreFromBundle(final DelegateDataStoreSpec spec, final Bundle bundle) {
        LOG.info("Trying to create data store with spec {} and bundle {}", spec, bundle.getSymbolicName());
        if (bundle.getState() == Bundle.ACTIVE) {
            try {
                LOG.info("Getting new class instance for {} from bundle {}", spec.getClassName(), bundle.getSymbolicName());
                Class dsClass = bundle.loadClass(spec.getClassName());
                DataStore ds = (DataStore) dsClass.newInstance();
                try {
                    Method setPropertiesMethod = dsClass.getMethod("setProperties", Properties.class);
                    setPropertiesMethod.invoke(ds, spec.getProperties());
                    if (ds.getClass().getSimpleName().equals("OakCachingFDS")) {
                        // TODO:  This all needs to be fixed.  We need a way to
                        // delegate property construction of a data store to the
                        // data store itself, not to the service class. -MR
                        String cs = spec.getProperties().getProperty("cacheSize", null);
                        long cacheSize = null == cs ? 0L : Long.parseLong(cs);
                        String fsBackendPath = spec.getProperties().getProperty("path", null);
                        if (null == fsBackendPath) {
                            fsBackendPath = spec.getProperties().getProperty("homeDir", null);
                        }
                      //spec.properties.remove("path");
                        spec.removeProperty("path");
                        //spec.properties.put("fsBackendPath", fsBackendPath);
                        spec.addProperty("fsBackendPath", fsBackendPath);
                        //spec.properties.put("cacheSize", cacheSize);
                        spec.addProperty("cacheSize", cacheSize);
                        String cachePath = spec.getProperties().getProperty("cachepath", null);
                        if (null != cachePath) {
                            //spec.properties.remove("cachePath");
                            spec.removeProperty("cachePath");
                            //spec.properties.put("path", cachePath);
                            spec.addProperty("path", cachePath);
                        }
                    }

                    Properties specProps = spec.getProperties();
                    String path = specProps.containsKey("path") ? specProps.getProperty("path") :
                            (specProps.containsKey("repository.home") ? specProps.getProperty("repository.home") :
                                    specProps.getProperty("homeDir"));
                    if (null != path) {
                        Method setPathMethod = dsClass.getMethod("setPath", String.class);
                        setPathMethod.invoke(ds, path);
                    }
                }
                catch (NoSuchMethodException | InvocationTargetException e) { }
                return ds;
            } catch (InstantiationException e) {
                LOG.warn(String.format("InstantiationException while trying to create data store %s", spec.getDataStoreName()), e);
            } catch (IllegalAccessException e) {
                LOG.warn(String.format("IllegalAccessException while trying to create data store %s", spec.getDataStoreName()), e);
            } catch (ClassNotFoundException e) {
                LOG.warn(String.format("No class found for data store %s", spec.getDataStoreName()), e);
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
            return new DataIdentifier(reference);
        }
        return null;
    }

    @Override
    public void init(String homeDir) throws RepositoryException {
        if (path == null) {
            path = homeDir + "/compositeds";
        }
        path = FilenameUtils.normalizeNoEndSeparator(new File(path).getAbsolutePath());
        LOG.debug("Root path is {}", path);
        this.rootDirectory = new File(path);
        this.tmp = new File(rootDirectory, "tmp");
        if (! this.tmp.exists() && ! this.tmp.mkdirs() ) {
            LOG.warn("Unable to create temporary directory");
        }

        // Attempt to initialize all delegates.
        LOG.debug("Using traversal strategy: {}", traversalStrategy);
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        RepositoryException aggregateException = null;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            LOG.debug("Attempting to initialize {}", ds);
            try {
                ds.init(homeDir);
            }
            catch (RepositoryException e) {
                if (null == aggregateException) {
                    aggregateException = new RepositoryException(
                            "Aggregate repository exception created in init",
                            e);
                }
                aggregateException.addSuppressed(e);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
    }

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        DataRecord result = null;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(identifier);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            result = ds.getRecordIfStored(identifier);
            if (null != result) {
                break;
            }
        }
        return result;
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        DataRecord result = getRecordIfStored(identifier);
        if (null == result) {
            throw new DataStoreException(String.format("No matching record for identifier %s", identifier.toString()));
        }
        return result;
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        return getRecordIfStored(getIdentifierFromReference(reference));
    }

    /**
     * Adds a new blob to a CompositeDataStore by invoking addRecord() on an appropriate
     * delegate blob store.  It is the responsibility of the {@link}DelegateTraversal to
     * select the delegate that is chosen to which the record will be added.
     *
     * However, some delegate traversal strategies will want to know whether the record
     * already exists in one of the delegates before completing the addition (either to
     * simply return the existing DataRecord from the delegate that already has the
     * record being added, or to move the record to a higher priority delegate, or
     * whatever).  But it is impossible to know whether the record exists without first
     * determining the blob ID for the stream, which can only be determined by reading
     * the entire stream and computing a content hash (content deduplication).
     * This means that the addRecord implementation for composite blob store must:
     * *Read the entire input stream to a temporary file and compute the blob ID.
     * *Ask the delegate traversal strategy to select a writable delegate to add the
     * record to for the provided blob ID.
     * *Delegate the operation to the selected delegate blob store.
     *
     * Prior to calling the delegate, CompositeDataStore will read the incoming stream,
     * save the stream to a temporary file, and compute the appropriate
     * {@link}DataIdentifier for the stream.  If the selected delegate implements
     * {@link}CompositeDataStoreAware, CompositeDataStore will invoke the appropriate
     * addRecord() method on the delegate using the existing {@link}DataIdentifier and
     * the temporary file.  If the delegate does not implement
     * {@link}CompositeDataStoreAware, the delegate will be given an input stream
     * from the temporary file and the delegate will be required to compute the
     * {@link}DataIdentifier from the stream again.
     *
     * @param stream The binary data being added to the CompositeDataStore.
     * @return {@link}DataRecord for the new binary added.  If the binary already existed,
     * this is the {@link}DataRecord for the already existing binary.
     * @throws DataStoreException if the selected delegate data store could not be accessed.
     */
    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        return this.addRecord(stream, new BlobOptions());
    }

    /**
     * Adds a new blob to a CompositeDataStore by invoking addRecord() on an appropriate
     * delegate blob store, and using the supplied {@link}BlobOptions.
     *
     * @param stream The binary data being added to the CompositeDataStore.
     * @param options Options relevant to choosing the delegate and adding the record.
     * @return {@link}DataRecord for the new binary added.  If the binary already existed,
     * this is the {@link}DataRecord for the already existing binary.
     * @throws DataStoreException if the selected delegate data store could not be accessed.
     */
    @Override
    public DataRecord addRecord(InputStream stream, BlobOptions options) throws DataStoreException {
        DataIdentifierCreationResult result;
        try {
            result = DataIdentifierFactory.createIdentifier(stream, tmp);
        }
        catch (NoSuchAlgorithmException nsae) {
            throw new DataStoreException("Unable to create identifier from blob stream", nsae);
        }
        catch (IOException ioe) {
            throw new DataStoreException("Unable to save blob stream to temporary file", ioe);
        }

        // NOTE:  There is discussion going on as to whether we should update the last modified
        // time of this object in a read-only delegate if it exists there.  One train of thought
        // is that we should do this, keeping only a single reference instead of creating a copy
        // of the same reference in a writable delegate which is what this current implementation
        // would do.  Garbage collection should work fine in either case, as the references
        // end up not being shared.  It's just less efficient.
        //
        // The problem is knowing for sure that the update to the read-only delegate will only
        // modify the last modified time of the blob.  JCR specifies that addRecord will add
        // the binary or update the last modified time if the binary already exists.  There is
        // potential for a race condition between the time we would check the delegate to see
        // if the binary exists and the time we call addRecord().  If the binary existed but
        // then was deleted before addRecord() was called, that would have the effect of
        // creating the blob again, thus actually modifying the read-only delegate.  There is
        // no API for doing this in the DataStore or related interfaces.
        //
        // For now I'm settling on the idea of having two copies, since I believe it will
        // at least behave consistently and not risk modifying the read-only delegate.
        // -MR
        DataStore dataStore = traversalStrategy.selectWritableDelegate(result.getIdentifier());
        if (null == dataStore) {
            throw new DataStoreException(
                    String.format("No writable delegate data store for identifier %s",
                            result.getIdentifier()));
        }
        return writeRecord(dataStore, result.getIdentifier(), result.getTmpFile(), options);
    }

    private DataRecord writeRecord(DataStore dataStore, DataIdentifier identifier, File tmpFile, BlobOptions options) throws DataStoreException {
        try {
            if (!(dataStore instanceof CompositeDataStoreAware)) {
                LOG.warn("Inefficient blob store delegation:  {} using delegate {} which is not an instance of {}",
                        this.getClass().getSimpleName(),
                        dataStore.getClass().getSimpleName(),
                        CompositeDataStoreAware.class.getSimpleName());
                LOG.warn("Consider rewriting {} to implement {}",
                        dataStore.getClass().getSimpleName(),
                        CompositeDataStoreAware.class.getSimpleName());
                if (dataStore instanceof TypedDataStore) {
                    return ((TypedDataStore) dataStore).addRecord(new FileInputStream(tmpFile), options);
                }
                return dataStore.addRecord(new FileInputStream(tmpFile));
            }
            if (dataStore instanceof TypedDataStore) {
                return ((CompositeDataStoreAware) dataStore).addRecord(identifier, tmpFile, options);
            }
            return ((CompositeDataStoreAware) dataStore).addRecord(identifier, tmpFile);
        }
        catch (Exception e) {
            LOG.error("Error adding record");
            throw new DataStoreException("Error adding record", e);
        }
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(identifier, DelegateTraversalOptions.RW_ONLY);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof MultiDataStoreAware) {
                ((MultiDataStoreAware) ds).deleteRecord(identifier);
            }
        }
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
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(DelegateTraversalOptions.RW_ONLY);
        int nDeleted = 0;
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            try {
                nDeleted += iter.next().deleteAllOlderThan(min);
            }
            catch (DataStoreException e) {
                if (null == aggregateException) {
                    aggregateException = new DataStoreException(
                            "Aggregate data store exception created in deleteAllOlderThan",
                            e);
                }
                aggregateException.addSuppressed(e);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
        return nDeleted;
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        // Attempt to iterate through all identifiers in all delegates.
        Iterator<DataIdentifier> result = null;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            try {
                if (null == result) {
                    result = iter.next().getAllIdentifiers();
                } else {
                    result = Iterators.concat(result, iter.next().getAllIdentifiers());
                }
            }
            catch (DataStoreException e) {
                if (null == aggregateException) {
                    aggregateException = new DataStoreException(
                            "Aggregate data store exception created in getAllIdentifiers",
                            e);
                }
                aggregateException.addSuppressed(e);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
        return result;
    }


    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        Iterator<DataRecord> result = null;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                if (null == result) {
                    result = ((SharedDataStore) ds).getAllRecords();
                }
                else {
                    result = Iterators.concat(((SharedDataStore) ds).getAllRecords());
                }
            }
        }
        return result;
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(identifier);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                DataRecord record = ((SharedDataStore) ds).getRecordForId(identifier);
                if (null != record) {
                    return record;
                }
            }
        }
        return null;
    }

    @Override
    public Type getType() {
        return Type.SHARED;
    }

    @Override
    public int getMinRecordLength() {
        return traversalStrategy.getMinRecordLength();
    }

    @Override
    public void close() throws DataStoreException {
        // Attempt to close all delegates.
        // If an exception is thrown, catch it and continue trying to
        // close the remaining delegates.
        // Rethrow the first caught exception if there is one.
        List<DataStoreException> exceptionsCaught = Lists.newArrayList();
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();

        DataStoreException aggregateException = null;

        while (iter.hasNext()) {
            try {
                iter.next().close();
            }
            catch (DataStoreException dse) {
                if (null == aggregateException) {
                    aggregateException = new DataStoreException(
                            "Aggregate data store exception created in close",
                            dse);
                }
                aggregateException.addSuppressed(dse);
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
    }

    @Override
    public void clearInUse() {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator();
        while (iter.hasNext()) {
            iter.next().clearInUse();
        }
    }

    // Metadata records apply to all delegates, including read-only delegates.
    // This is necessary in order to perform GC across shared data stores.
    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(DelegateTraversalOptions.RORW);
        DataStoreException aggregateException = null;
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                try {
                    ((SharedDataStore) ds).addMetadataRecord(stream, name);
                }
                catch (DataStoreException dse) {
                    if (null == aggregateException) {
                        aggregateException = new DataStoreException(
                                "Aggregate data store exception created in addMetadataRecord",
                                dse
                        );
                    }
                    aggregateException.addSuppressed(dse);
                }
            }
        }
        if (null != aggregateException) {
            throw aggregateException;
        }
    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        try {
            addMetadataRecord(new FileInputStream(f), name);
        }
        catch (IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(DelegateTraversalOptions.RORW);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                DataRecord result = ((SharedDataStore) ds).getMetadataRecord(name);
                if (null != result) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        List<DataRecord> records = Lists.newArrayList();
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(DelegateTraversalOptions.RORW);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                records.addAll(((SharedDataStore) ds).getAllMetadataRecords(prefix));
            }
        }
        return records;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        boolean result = false;
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(DelegateTraversalOptions.RORW);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                result = result || ((SharedDataStore) ds).deleteMetadataRecord(name);
            }
        }
        return result;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        Iterator<DataStore> iter = traversalStrategy.getDelegateIterator(DelegateTraversalOptions.RORW);
        while (iter.hasNext()) {
            DataStore ds = iter.next();
            if (ds instanceof SharedDataStore) {
                ((SharedDataStore) ds).deleteAllMetadataRecords(prefix);
            }
        }
    }

    @Override
    public void bundleChanged(BundleEvent event) {
        if (event.getType() == BundleEvent.STARTED) {
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
