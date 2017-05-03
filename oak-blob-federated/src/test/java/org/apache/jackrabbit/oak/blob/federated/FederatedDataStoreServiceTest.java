package org.apache.jackrabbit.oak.blob.federated;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class FederatedDataStoreServiceTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void testCreateDataStore() {
        FederatedDataStoreService service = new FederatedDataStoreService();
        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof FederatedDataStore);
    }

//    @Test
//    public void testCreateWithFileDataStore() {
//        FederatedDataStoreService service = new FederatedDataStoreService();
//        Map<String, Object> config = Maps.newHashMap();
//        config.put("dataStoreName", "org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore");
//        FederatedDataStore ds = (FederatedDataStore) service.createDataStore(context.componentContext(), config);
//        assertTrue(ds.getFederatedDataStores().get(0) instanceof DataStore);
//    }
}
