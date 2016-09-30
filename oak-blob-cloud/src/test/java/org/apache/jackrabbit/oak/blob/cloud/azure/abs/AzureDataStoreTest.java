package org.apache.jackrabbit.oak.blob.cloud.azure.abs;

import com.amazonaws.util.StringInputStream;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.*;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.Utils;
import org.apache.jackrabbit.oak.blob.cloud.azure.abs.AzureBlobStoreBackend;
import org.apache.jackrabbit.oak.blob.cloud.azure.abs.AzureDataStore;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import java.io.*;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import static java.awt.Color.red;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AzureDataStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private static Properties props;
    private static byte[] testBuffer;
    private TestAzureDataStore ds;
    private AzureBlobStoreBackend backend;

    @BeforeClass
    public static void setupClass() {
        String configFile = System.getProperty(TestCaseBase.CONFIG,
                "./src/test/resources/azure.properties");
        try {
            props = Utils.readConfig(configFile);
        }
        catch (IOException e) {
            // Ignore all tests if no properties file found for Azure
            assumeTrue(false);
        }

        testBuffer = "test".getBytes();
    }

    @Before
    public void setup() throws IOException, RepositoryException {
        ds = new TestAzureDataStore(props);
        ds.init(folder.newFolder().getAbsolutePath());
        backend = (AzureBlobStoreBackend) ds.getBackend();
    }

    @After
    public void teardown() {
        ds = null;
    }

    private void waitForUpload(final TestAzureDataStore ds) {
        waitForUpload(ds, 1);
    }

    private void waitForUpload(final TestAzureDataStore ds, int expectedUploads) {
        while (ds.getSuccessfulUploads() < expectedUploads) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) { }
        }
    }

    @Test
    public void testCreateAndDeleteBlobHappyPath() throws DataStoreException, IOException {
        ds.resetSuccessfulUploads();
        final DataRecord uploadedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier = uploadedRecord.getIdentifier();
        //DataIdentifier identifier = new DataIdentifier("a94a8fe5ccb19ba61c4c0873d391e987982fbbd3");
        waitForUpload(ds);
        assertTrue(backend.exists(identifier));
        assertTrue(0 != uploadedRecord.getLastModified());
        assertEquals(testBuffer.length, uploadedRecord.getLength());

        final DataRecord retrievedRecord = ds.getRecord(identifier);
        assertEquals(retrievedRecord.getLength(), uploadedRecord.getLength());
        assertEquals(retrievedRecord.getLastModified(), uploadedRecord.getLastModified());
        assertTrue(retrievedRecord.getIdentifier().toString().equals(identifier.toString()));

        StringWriter writer = new StringWriter();
        org.apache.commons.io.IOUtils.copy(retrievedRecord.getStream(), writer, "utf-8");
        String s = writer.toString();
        assertTrue(s.equals(new String(testBuffer)));

        ds.deleteRecord(identifier);
        assertFalse(backend.exists(uploadedRecord.getIdentifier()));
    }

    @Test
    public void testListBlobs() throws UnsupportedEncodingException, DataStoreException {
        ds.resetSuccessfulUploads();
        final Set<DataIdentifier> identifiers = Sets.newHashSet();
        final Set<String> testStrings = Sets.newHashSet("test1", "test2", "test3");

        for (String s : testStrings) {
            identifiers.add(ds.addRecord(new StringInputStream(s)).getIdentifier());
        }
        waitForUpload(ds, testStrings.size());

        Iterator<DataIdentifier> iter = ds.getAllIdentifiers();
        while (iter.hasNext()) {
            DataIdentifier identifier = iter.next();
            assertTrue(identifiers.contains(identifier));
            ds.deleteRecord(identifier);
        }
    }

    @Test
    public void testDeleteAllOlderThan() throws UnsupportedEncodingException, DataStoreException {
        ds.resetSuccessfulUploads();
        final Set<DataRecord> records = Sets.newHashSet();
        final Set<String> testStrings = Sets.newHashSet("test1", "test2", "test3");

        for (String s : testStrings) {
            records.add(ds.addRecord(new StringInputStream(s)));
            try {
                Thread.sleep(1001);
                // Azure stores timestamps in millisecond precision,
                // but appears to truncate those to the nearest second.
                // So records have to be stored more than 1000
                // milliseconds apart in order for the test to pass.
            }
            catch (InterruptedException e) { }
        }
        waitForUpload(ds, testStrings.size());

        long deleteAllOlderThanMillis = 0;
        for (DataRecord r : records) {
            if (r.getLastModified() > deleteAllOlderThanMillis) {
                deleteAllOlderThanMillis = r.getLastModified();
            }
        }

        int recordsDeleted = ds.deleteAllOlderThan(deleteAllOlderThanMillis);
        assertEquals(2, recordsDeleted);

        Iterator<DataIdentifier> iter = ds.getAllIdentifiers();
        int recordsNotDeleted = 0;
        while (iter.hasNext()) {
            ds.deleteRecord(iter.next());
            ++recordsNotDeleted;
        }

        assertEquals(1, recordsNotDeleted);
    }

    private static class TestAzureDataStore extends AzureDataStore {
        public TestAzureDataStore(final Properties properties) {
            this.properties = properties;
        }

        private int successfulUploads = 0;
        public int getSuccessfulUploads() { return successfulUploads; }
        public void resetSuccessfulUploads() { successfulUploads = 0; }
        @Override
        public void onSuccess(AsyncUploadResult result) {
            ++successfulUploads;
            super.onSuccess(result);
        }
    }
}
