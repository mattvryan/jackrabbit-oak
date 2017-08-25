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
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IntelligentDelegateTraversal implements DelegateTraversal {
    private static Logger LOG = LoggerFactory.getLogger(IntelligentDelegateTraversal.class);

    private static final String DATASTORE = "dataStore";

    private Map<String, DataStore> dataStoresByBundleName = Maps.newConcurrentMap();

    //private Map<DelegateDataStoreFilter, DataStore> filteredWritableDataStores = Maps.newConcurrentMap();
    private List<DataStore> nonFilteredWritableDataStores = Lists.newArrayList();
    //private Map<DelegateDataStoreFilter, DataStore> filteredReadOnlyDataStores = Maps.newConcurrentMap();
    private List<DataStore> nonFilteredReadOnlyDataStores = Lists.newArrayList();
    // FUTURE
    //private Map<DelegateDataStoreFilter, DataStore> filteredColdStorageDataStores = Maps.newConcurrentMap();
    //private List<DataStore> nonFilteredColdStorageDataStores = Lists.newArrayList();

    private DelegateMinRecordLengthChooser minRecordLengthChooser = new GuaranteedMinRecordLengthChooser();

    @Override
    public void addDelegateDataStore(final DataStore ds, final DelegateDataStoreSpec spec) {
//        if (spec.isColdStorage()) {
//            if (spec.hasFilter()) {
//                filteredColdStorageDataStores.put(spec.getFilter(), ds);
//            }
//            else {
//                nonFilteredColdStorageDataStores.add(ds);
//            }
//        }
//        else
        if (spec.isReadOnly()) {
//            if (spec.hasFilter()) {
//                filteredReadOnlyDataStores.put(spec.getFilter(), ds);
//            }
//            else {
                nonFilteredReadOnlyDataStores.add(ds);
//            }
//        } else if (spec.hasFilter()) {
//            filteredWritableDataStores.put(spec.getFilter(), ds);
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
            for (List<DataStore> l : Lists.newArrayList(nonFilteredWritableDataStores,
                    nonFilteredReadOnlyDataStores)) {
                    //nonFilteredColdStorageDataStores)) {
                l.remove(toRemove);
            }
//            for (Map<DelegateDataStoreFilter, DataStore> m : Lists.newArrayList(filteredWritableDataStores, filteredReadOnlyDataStores, filteredColdStorageDataStores)) {
//                m.entrySet().removeIf(entry -> entry.getValue().equals(toRemove));
//            }
        }
    }

    @Override
    public boolean hasDelegate() {
        return (//! filteredWritableDataStores.isEmpty()    ||
                ! nonFilteredWritableDataStores.isEmpty() ||
                //! filteredReadOnlyDataStores.isEmpty()    ||
                ! nonFilteredReadOnlyDataStores.isEmpty()
                //! filteredColdStorageDataStores.isEmpty() ||
                //! nonFilteredColdStorageDataStores.isEmpty()
        );
    }

    @Override
    public DataStore selectWritableDelegate(final DataIdentifier identifier) {
        Iterator<DataStore> iter = getDelegateIterator(identifier, DelegateTraversalOptions.RW_ONLY);
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
        }
        LOG.error("No writable delegates configured - could not select a writable delegate");
        return null;
    }

    @Override
    public Iterator<DataStore> getDelegateIterator() {
        return getIterator(Optional.empty(), Optional.empty());
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DataIdentifier identifier) {
        return getIterator(Optional.of(identifier), Optional.empty());
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DelegateTraversalOptions options) {
        return getIterator(Optional.empty(), Optional.of(options));
    }

    @Override
    public Iterator<DataStore> getDelegateIterator(final DataIdentifier identifier, final DelegateTraversalOptions options) {
        return getIterator(Optional.of(identifier), Optional.of(options));
    }

    private Iterator<DataStore> getIterator(final Optional<DataIdentifier> identifierOptional, final Optional<DelegateTraversalOptions> optionsOptional) {
        // DataIdentifiers and DelegateTraversalOptions are used to limit the number of data stores we iterate.
        // If these are not provided we return all iterators.

//        if (identifierOptional.isPresent()) {
//            // Not supported for now
//            List<DataStore> matchingDataStores = Lists.newArrayList();
//            for (Map<DelegateDataStoreFilter, DataStore> m : Lists.newArrayList(filteredWritableDataStores, filteredReadOnlyDataStores, filteredColdStorageDataStores)) {
//                for (Map.Entry<DelegateDataStoreFilter, DataStore> entry : m.entrySet()) {
//                    if (entry.getKey().matches(identifier)) {
//                        matchingDataStores.add(entry.getValue());
//                    }
//                }
//            }
//            return matchingDataStores.iterator();
//        }

        if (optionsOptional.isPresent()) {
            DelegateTraversalOptions options = optionsOptional.get();
            Iterator<DataStore> i;
            if (DelegateTraversalOptions.RORW_OPTION.RO_ONLY == options.readWriteOption) {
                i = nonFilteredReadOnlyDataStores.iterator();
            }
            else if (DelegateTraversalOptions.RORW_OPTION.RW_ONLY == options.readWriteOption) {
                i = nonFilteredWritableDataStores.iterator();
            }
            else {
                i = Iterators.concat(nonFilteredWritableDataStores.iterator(),
                        nonFilteredReadOnlyDataStores.iterator());
            }
            // Add cold storage iterators here, when we support them

            return i;
        }
        else {
            return Iterators.concat(
                    nonFilteredWritableDataStores.iterator(),
                    nonFilteredReadOnlyDataStores.iterator()
            );
        }
    }

    @Override
    public int getMinRecordLength() {
        return minRecordLengthChooser.getMinRecordLength(this);
    }
}
