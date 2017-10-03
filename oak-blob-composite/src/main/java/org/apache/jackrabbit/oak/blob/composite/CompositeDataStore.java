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

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.BundleListener;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.FrameworkListener;
import org.osgi.service.component.ComponentContext;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompositeDataStore implements DataStore, SharedDataStore, TypedDataStore, MultiDataStoreAware, BundleListener, FrameworkListener {

    public void setProperties(final Map<String, Object> config, final ComponentContext context) {

    }

    void addActiveDataStores(final BundleContext context) {

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

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {

    }

    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {

    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {

    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        return null;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        return null;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {

    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        return null;
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier id) throws DataStoreException {
        return null;
    }

    @Override
    public Type getType() {
        return null;
    }

    @Override
    public DataRecord addRecord(InputStream input, BlobOptions options) throws DataStoreException {
        return null;
    }

    @Override
    public void bundleChanged(BundleEvent event) {

    }

    @Override
    public void frameworkEvent(FrameworkEvent event) {

    }
}