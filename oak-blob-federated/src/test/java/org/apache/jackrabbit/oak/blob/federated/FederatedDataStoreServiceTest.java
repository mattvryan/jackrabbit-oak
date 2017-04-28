package org.apache.jackrabbit.oak.blob.federated;

import org.apache.jackrabbit.core.data.DataStore;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
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
        Map<String, Object> config = new HashMap<String, Object>();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertTrue(ds instanceof FederatedDataStore);
    }
}
