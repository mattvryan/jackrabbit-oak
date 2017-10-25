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

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.InMemoryDataStore;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.ServiceReference;

import javax.jcr.RepositoryException;
import java.io.File;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class CompositeDataStoreServiceTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    private DataStoreProvider createDelegateDataStore() {
        DataStoreProvider ds = new InMemoryDataStoreProvider();
        return ds;
    }

    private Map<String, Object> createConfig() {
        return createConfig("localRole");
    }

    private Map<String, Object> createConfig(String role) {
        Map<String, Object> config = Maps.newHashMap();
        config.put(DataStoreProvider.ROLE, role);
        return config;
    }

    private void verifyDelegateCount(Iterator<CompositeDataStoreDelegate> iter, int expectedCount) {
        if (expectedCount > 0) {
            assertTrue(iter.hasNext());
            int actualCount = 0;
            while (iter.hasNext()) {
                ++actualCount;
                iter.next();
            }
            assertEquals(expectedCount, actualCount);
        } else {
            assertFalse(iter.hasNext());
        }
    }

    private void verifyRegistrationState(OsgiContext context, boolean shouldBeActive) {
        ServiceReference ref = context.bundleContext().getServiceReference(CompositeDataStore.class.getName());
        if (shouldBeActive) {
            assertNotNull(ref);
        } else {
            assertNull(ref);
        }
    }

    private void verifyActiveReg(OsgiContext context) { verifyRegistrationState(context, true); }
    private void verifyInactiveReg(OsgiContext context) { verifyRegistrationState(context, false);}

    @Test
    public void testCreateDataStore() throws RepositoryException {
        CompositeDataStoreService service = new TestableCompositeDataStoreService();

        service.addDelegateDataStore(
                createDelegateDataStore(),
                createConfig()
        );

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof CompositeDataStore);

        ServiceReference ref = context.bundleContext().getServiceReference(CompositeDataStore.class.getName());
        assertNotNull(ref);
    }

    @Test
    public void testCreateDataStoreWithoutActiveDelegatesReturnsNull() {
        CompositeDataStoreService service = new TestableCompositeDataStoreService();

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertNull(ds);

        ServiceReference ref = context.bundleContext().getServiceReference(CompositeDataStore.class.getName());
        assertNull(ref);
    }

    @Test
    public void testCreateDataStoreDeferredDataStoreCreationWorks() {
        CompositeDataStoreService service = new TestableCompositeDataStoreService();

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertNull(ds);

        ServiceReference ref = context.bundleContext().getServiceReference(CompositeDataStore.class.getName());
        assertNull(ref);

        service.addDelegateDataStore(
                createDelegateDataStore(),
                createConfig()
        );

        verifyDelegateCount(service.getDelegateIterator(), 1);
        verifyActiveReg(context);
    }

    @Test
    public void testCreateDataStoreDeferredDataStoreCreationIsAdditive() {
        CompositeDataStoreService service = new TestableCompositeDataStoreService();

        service.addDelegateDataStore(
                createDelegateDataStore(),
                createConfig()
        );

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof CompositeDataStore);

        verifyDelegateCount(service.getDelegateIterator(), 1);
        verifyActiveReg(context);

        service.addDelegateDataStore(
                createDelegateDataStore(),
                createConfig()
        );

        assertEquals(ds, service.getDataStore());

        verifyDelegateCount(service.getDelegateIterator(), 2);
        verifyActiveReg(context);
    }

    @Test
    public void testRemoveDelegateDataStoreUnregistersService() {
        CompositeDataStoreService service = new TestableCompositeDataStoreService();

        DataStoreProvider dsProvider = createDelegateDataStore();
        service.addDelegateDataStore(
                dsProvider,
                createConfig()
        );

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof CompositeDataStore);

        verifyDelegateCount(service.getDelegateIterator(), 1);
        verifyActiveReg(context);

        service.removeDelegateDataStore(dsProvider);

        verifyDelegateCount(service.getDelegateIterator(), 0);
        verifyInactiveReg(context);
    }

    @Test
    public void testRemoveDelegateDataStoreTypeRemovesAllMatchingDelegates() {
        CompositeDataStoreService service = new TestableCompositeDataStoreService();

        DataStoreProvider ds1 = new InMemoryDataStoreProvider();
        DataStoreProvider ds2 = new InMemoryDataStoreProvider();
        DataStoreProvider ds3 = new OtherInMemoryDataStoreProvider();

        service.addDelegateDataStore(ds1, createConfig("local1"));
        service.addDelegateDataStore(ds2, createConfig("local2"));
        service.addDelegateDataStore(ds3, createConfig("remote1"));

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof CompositeDataStore);

        verifyDelegateCount(service.getDelegateIterator(), 3);
        verifyActiveReg(context);

        service.removeDelegateDataStore(ds3);

        verifyDelegateCount(service.getDelegateIterator(), 2);
        verifyActiveReg(context);

        service.addDelegateDataStore(ds3, createConfig());
        service.removeDelegateDataStore(ds1);

        verifyDelegateCount(service.getDelegateIterator(), 1);
        verifyActiveReg(context);

        service.removeDelegateDataStore(ds3);

        verifyDelegateCount(service.getDelegateIterator(), 0);
        verifyInactiveReg(context);
    }

    static class TestableCompositeDataStoreService extends CompositeDataStoreService {
        @Override
        protected BlobStoreStats getBlobStoreStats(StatisticsProvider sp) {
            return mock(BlobStoreStats.class);
        }
    }

    static class OtherInMemoryDataStoreProvider extends InMemoryDataStore implements DataStoreProvider {
        @Override
        public DataStore getDataStore() {
            return this;
        }
    }
}
