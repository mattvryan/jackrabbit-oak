package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class CompositeDataStoreServiceTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void testCreateDataStore() {
        CompositeDataStoreService service = new CompositeDataStoreService();
        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof CompositeDataStore);
    }

//    @Test
//    public void testCreateWithFileDataStore() {
//        CompositeDataStoreService service = new CompositeDataStoreService();
//        Map<String, Object> config = Maps.newHashMap();
//        config.put("dataStoreName", "org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore");
//        CompositeDataStore ds = (CompositeDataStore) service.createDataStore(context.componentContext(), config);
//        assertTrue(ds.getCompositeDataStores().get(0) instanceof DataStore);
//    }
}
