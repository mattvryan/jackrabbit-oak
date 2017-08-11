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

package org.apache.jackrabbit.oak.blob.composite

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IntelligentDelegateTraversalStrategy implements DelegateTraversalStrategy {
    private static Logger LOG = LoggerFactory.getLogger(IntelligentDelegateTraversalStrategy.class);

    private static final String DATASTORE = "dataStore";

    private Map<String, DataStore> dataStoresByBundleName = Maps.newConcurrentMap();

    private Map<DelegateDataStoreFilter, DataStore> filteredWritableDataStores = Maps.newConcurrentMap();
    private List<DataStore> nonFilteredWritableDataStores = Lists.newArrayList();
    private Map<DelegateDataStoreFilter, DataStore> filteredReadOnlyDataStores = Maps.newConcurrentMap();
    private List<DataStore> nonFilteredReadOnlyDataStores = Lists.newArrayList();
    private Map<DelegateDataStoreFilter, DataStore> filteredColdStorageDataStores = Maps.newConcurrentMap();
    private List<DataStore> nonFilteredColdStorageDataStores = Lists.newArrayList();

    @Override
    public void addDelegateDataStore(final DataStore ds, final DelegateDataStoreSpec spec) {
        if (spec.isColdStorage()) {
            if (spec.hasFilter()) {
                filteredColdStorageDataStores.put(spec.getFilter(), ds);
            }
            else {
                nonFilteredColdStorageDataStores.add(ds);
            }
        }
        else if (spec.isReadOnly()) {
            if (spec.hasFilter()) {
                filteredReadOnlyDataStores.put(spec.getFilter(), ds);
            }
            else {
                nonFilteredReadOnlyDataStores.add(ds);
            }
        } else if (spec.hasFilter()) {
            filteredWritableDataStores.put(spec.getFilter(), ds);
        }
        else {
            nonFilteredWritableDataStores.add(ds);
        }
        dataStoresByBundleName.put(spec.getBundleName(), ds);
    }

    @Override
    public void removeDelegateDataStoresForBundle(Bundle bundle) {
        String bundleName = bundle.getSymbolicName();
        if (dataStoresByBundleName.containsKey(bundleName)) {
            DataStore toRemove = dataStoresByBundleName.get(bundleName);
            for (List<DataStore> l : Lists.newArrayList(nonFilteredWritableDataStores, nonFilteredReadOnlyDataStores, nonFilteredColdStorageDataStores)) {
                l.remove(toRemove);
            }
            for (Map<DelegateDataStoreFilter, DataStore> m : Lists.newArrayList(filteredWritableDataStores, filteredReadOnlyDataStores, filteredColdStorageDataStores)) {
                m.entrySet().removeIf(entry -> entry.getValue().equals(toRemove));
            }
        }
    }

    @Override
    public boolean hasDelegate() {
        return (!filteredWritableDataStores.isEmpty()    ||
                !nonFilteredWritableDataStores.isEmpty() ||
                !filteredReadOnlyDataStores.isEmpty()    ||
                !nonFilteredReadOnlyDataStores.isEmpty() ||
                !filteredColdStorageDataStores.isEmpty() ||
                !nonFilteredColdStorageDataStores.isEmpty() );
    }


    @Override
    public DataStore selectWritableDelegate(final DataIdentifier identifier) {
        DataStore firstMatchingFilteredDelegate = null;

        // First check any writable delegates that have filters that match.
        Iterator<DataStore> iter = getWritableDelegateIterator(identifier);
        while (iter.hasNext()) {
            DataStore writableDelegate = iter.next();
            try {
                if (null != writableDelegate.getRecordIfStored(identifier)) {
                    return writableDelegate;
                }
            }
            catch (DataStoreException dse) {
                LOG.warn("Unable to access delegate data store while selecting writable delegate", dse);
            }
            if (null != firstMatchingFilteredDelegate) {
                firstMatchingFilteredDelegate = writableDelegate;
            }
        }

        // Next check any writable delegates without filters
        Iterator<DataStore> unfilteredIterator = nonFilteredWritableDataStores.iterator();
        while (unfilteredIterator.hasNext()) {
            DataStore writableDelegate = unfilteredIterator.next();
            try {
                if (null != writableDelegate.getRecordIfStored(identifier)) {
                    return writableDelegate;
                }
            }
            catch (DataStoreException dse) {
                LOG.warn("Unable to access delegate data store while selecting writable delegate", dse);
            }
        }

        // Next check writable delegates that have filters that do not match.
        Iterator<Map.Entry<DelegateDataStoreFilter, DataStore>> filteredIterator = filteredWritableDataStores.entrySet().iterator();
        while (filteredIterator.hasNext()) {
            Map.Entry<DelegateDataStoreFilter, DataStore> entry = filteredIterator.next();
            try {
                if (!entry.getKey().matches(identifier) && null != entry.getValue().getRecordIfStored(identifier)) {
                    return entry.getValue();
                }
            }
            catch (DataStoreException dse) {
                LOG.warn("Unable to access delegate data store while selecting writable delegate", dse);
            }
        }

        if (null != firstMatchingFilteredDelegate) {
            return firstMatchingFilteredDelegate;
        }

        if (! nonFilteredWritableDataStores.isEmpty()) {
            return nonFilteredWritableDataStores.get(0);
        }

        // If we somehow haven't returned a delegate yet, return any available filtered one
        if (! filteredWritableDataStores.isEmpty()) {
            return filteredWritableDataStores.values().iterator().next();
        }

        // And if we still haven't returned a delegate, there aren't any writable delegates at all
        LOG.error("No writable delegates configured - could not select a writable delegate");
        return null;
    }

    @Override
    public Iterator<DataStore> getWritableDelegateIterator(final DataIdentifier identifier) {
        List<DataStore> dataStoreList = Lists.newArrayList();
        for (Map.Entry<DelegateDataStoreFilter, DataStore> entry : filteredWritableDataStores.entrySet()) {
            if (entry.getKey().matches(identifier)) {
                dataStoreList.add(entry.getValue());
            }
        }
        return dataStoreList.iterator();
    }

    @Override
    public Iterator<DataStore> getWritableDelegateIterator() {
        return Iterators.concat(nonFilteredWritableDataStores.iterator(), filteredColdStorageDataStores.values().iterator());
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DataIdentifier identifier) {
        List<DataStore> matchingDataStores = Lists.newArrayList();
        for (Map<DelegateDataStoreFilter, DataStore> m : Lists.newArrayList(filteredWritableDataStores, filteredReadOnlyDataStores, filteredColdStorageDataStores)) {
            for (Map.Entry<DelegateDataStoreFilter, DataStore> entry : m.entrySet()) {
                if (entry.getKey().matches(identifier)) {
                    matchingDataStores.add(entry.getValue());
                }
            }
        }
        return matchingDataStores.iterator();
    }

    @Override
    public Iterator<DataStore> getDelegateIterator() {
        return Iterators.concat(
                nonFilteredWritableDataStores.iterator(),
                nonFilteredReadOnlyDataStores.iterator(),
                filteredWritableDataStores.values().iterator(),
                filteredReadOnlyDataStores.values().iterator(),
                nonFilteredColdStorageDataStores.iterator(),
                filteredColdStorageDataStores.values().iterator()
        );
    }
}
