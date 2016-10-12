/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.abs;

import com.amazonaws.util.StringInputStream;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.*;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.Utils;
import org.joda.time.DateTime;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import java.io.*;
import java.net.URISyntaxException;
import java.security.DigestOutputStream;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.junit.Assert.*;
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
    public void teardown() throws InvalidKeyException, URISyntaxException, StorageException {
        ds = null;

        // Empty the container
        String connectionString = String.format(
                "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s",
                props.getProperty(AzureConstants.AZURE_ACCOUNT_NAME, ""),
                props.getProperty(AzureConstants.AZURE_ACCOUNT_KEY, ""));
        CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
        CloudBlobClient client = account.createCloudBlobClient();
        CloudBlobContainer azureContainer = client.getContainerReference(props.getProperty(AzureConstants.ABS_CONTAINER_NAME, ""));
        for (final ListBlobItem item : azureContainer.listBlobs()) {
            if (item instanceof CloudBlob) {
                ((CloudBlob)item).deleteIfExists();
            }
        }
    }

    private void waitForUpload(final TestAzureDataStore ds) throws IOException {
        waitForUpload(ds, 1);
    }

    private void waitForUpload(final TestAzureDataStore ds, int expectedUploads) throws IOException {
        int tries = 0;
        while (ds.getSuccessfulUploads() < expectedUploads) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) { }
            if (++tries > 5) {
                throw new IOException("Upload timed out");
            }
        }
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataRecord rhs)
            throws DataStoreException, IOException {
        validateRecord(record, contents, rhs.getIdentifier(), rhs.getLength(), rhs.getLastModified());
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataRecord rhs,
                                boolean lastModifiedEquals)
            throws DataStoreException, IOException {
        validateRecord(record, contents, rhs.getIdentifier(), rhs.getLength(), rhs.getLastModified(), lastModifiedEquals);
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataIdentifier identifier,
                                final long length,
                                final long lastModified)
            throws DataStoreException, IOException {
        validateRecord(record, contents, identifier, length, lastModified, true);
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataIdentifier identifier,
                                final long length,
                                final long lastModified,
                                final boolean lastModifiedEquals)
            throws DataStoreException, IOException {
        assertEquals(record.getLength(), length);
        if (lastModifiedEquals) {
            assertEquals(record.getLastModified(), lastModified);
        } else {
            assertTrue(record.getLastModified() > lastModified);
        }
        assertTrue(record.getIdentifier().toString().equals(identifier.toString()));
        StringWriter writer = new StringWriter();
        org.apache.commons.io.IOUtils.copy(record.getStream(), writer, "utf-8");
        assertTrue(writer.toString().equals(contents));
    }

    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static String getIdForInputStream(final InputStream in)
            throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
        try {
            IOUtils.copyLarge(in, output);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(in);
        }
        return encodeHexString(digest.digest());
    }

    @Ignore
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
        validateRecord(retrievedRecord, new String(testBuffer), uploadedRecord);

        ds.deleteRecord(identifier);
        assertFalse(backend.exists(uploadedRecord.getIdentifier()));
    }

    @Ignore
    @Test
    public void testCreateAndUpdateBlobHappyPath() throws DataStoreException, IOException {
        ds.resetSuccessfulUploads();
        final DataRecord uploadedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier = uploadedRecord.getIdentifier();
        waitForUpload(ds);
        assertTrue(backend.exists(identifier));

        final DataRecord retrievedRecord1 = ds.getRecord(identifier);
        validateRecord(retrievedRecord1, new String(testBuffer), uploadedRecord);

        byte[] modifiedBuffer = "modified".getBytes();
        File testFile = folder.newFile();
        copyInputStreamToFile(new ByteArrayInputStream(modifiedBuffer), testFile);
        backend.write(identifier, testFile);

        InputStream updatedIS = backend.read(identifier);
        StringWriter writer2 = new StringWriter();
        org.apache.commons.io.IOUtils.copy(updatedIS, writer2, "utf-8");
        assertTrue(writer2.toString().equals(new String(modifiedBuffer)));

        ds.deleteRecord(identifier);
        assertFalse(backend.exists(uploadedRecord.getIdentifier()));
    }

    @Ignore
    @Test
    public void testCreateAndReUploadBlob() throws DataStoreException, IOException {
        ds.resetSuccessfulUploads();
        final DataRecord createdRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier1 = createdRecord.getIdentifier();
        waitForUpload(ds);
        assertTrue(backend.exists(identifier1));

        final DataRecord record1 = ds.getRecord(identifier1);
        validateRecord(record1, new String(testBuffer), createdRecord);

        try { Thread.sleep(1001); } catch (InterruptedException e) { }

        final DataRecord updatedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
        DataIdentifier identifier2 = updatedRecord.getIdentifier();
        waitForUpload(ds);
        assertTrue(backend.exists(identifier2));

        assertTrue(identifier1.toString().equals(identifier2.toString()));
        validateRecord(ds.getRecord(identifier2), new String(testBuffer), createdRecord);

        ds.deleteRecord(identifier1);
        assertFalse(backend.exists(createdRecord.getIdentifier()));
    }

    @Ignore
    @Test
    public void testListBlobs() throws DataStoreException, IOException {
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

    @Ignore
    @Test
    public void testDeleteAllOlderThan() throws DataStoreException, IOException {
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


    ////
    // Backend Tests
    ////

    private void validateRecordData(final Backend backend,
                                    final DataIdentifier identifier,
                                    int expectedSize,
                                    final InputStream expected) throws IOException, DataStoreException {
        byte[] blobData = new byte[expectedSize];
        backend.read(identifier).read(blobData);
        byte[] expectedData = new byte[expectedSize];
        expected.read(expectedData);
        for (int i=0; i<expectedSize; i++) {
            assertEquals(expectedData[i], blobData[i]);
        }
    }

    // Write (Backend)

    @Ignore
    @Test
    public void testBackendWriteDifferentSizedRecords() throws IOException, NoSuchAlgorithmException, DataStoreException {
        boolean async = true;
        do {
            async = ! async;
            TestAsyncCallback callback = new TestAsyncCallback();

            // Sizes are chosen as follows:
            // 0 - explicitly test zero-size file
            // 10 - very small file
            // 1000 - under 4K (a reasonably expected stream buffer size)
            // 4100 - over 4K but under 8K and 16K (other reasonably expected stream buffer sizes)
            // 16500 - over 8K and 16K but under 64K (another reasonably expected stream buffer size)
            // 66000 - over 64K but under 128K (probably the largest reasonably expected stream buffer size)
            // 132000 - over 128K
            for (int size : Lists.newArrayList(0, 10, 1000, 4100, 16500, 66000, 132000)) {
                File testFile = folder.newFile();
                copyInputStreamToFile(randomStream(size, size), testFile);
                DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
                if (async) {
                    callback.reset();
                    backend.writeAsync(identifier, testFile, callback);
                    assertTrue(callback.waitForUploadSuccess());
                }
                else {
                    backend.write(identifier, testFile);
                }
                assertTrue(backend.exists(identifier));
                assertEquals(size, backend.getLength(identifier));
                assertTrue(backend.getLastModified(identifier) != 0);

                validateRecordData(backend, identifier, size, new FileInputStream(testFile));

                if (0 != size) {
                    // Modify with same size different data, bigger size, smaller size
                    int offset = ((int) (size * .2));
                    for (int newSize : Lists.newArrayList(size, size + offset, size - offset)) {
                        File newFile = folder.newFile();
                        copyInputStreamToFile(randomStream(0, newSize), newFile);
                        if (async) {
                            callback.reset();
                            backend.writeAsync(identifier, newFile, callback);
                            assertTrue(callback.waitForUploadSuccess());
                        }
                        else {
                            backend.write(identifier, newFile);
                        }
                        assertTrue(backend.exists(identifier));
                        assertEquals(newSize, backend.getLength(identifier));
                        validateRecordData(backend, identifier, newSize, new FileInputStream(newFile));
                    }
                }

                backend.deleteRecord(identifier);
                assertFalse(backend.exists(identifier));
            }
        }
        while (!async);
    }

    @Ignore
    @Test
    public void testBackendWriteRecordNullIdentifierThrowsNullPointerException() throws IOException, DataStoreException{
        boolean async = true;
        do {
            async = !async;

            DataIdentifier identifier = null;
            File testFile = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile);
            try {
                if (async) {
                    backend.writeAsync(identifier, testFile, new TestAsyncCallback());
                }
                else {
                    backend.write(identifier, testFile);
                }
                fail();
            } catch (NullPointerException e) {
                assertEquals("identifier", e.getMessage());
            }
        }
        while (!async);
    }

    @Ignore
    @Test
    public void testBackendWriteRecordNullFileThrowsNullPointerException() throws DataStoreException {
        boolean async = true;
        do {
            async = !async;

            File testFile = null;
            DataIdentifier identifier = new DataIdentifier("fake");
            try {
                if (async) {
                    backend.writeAsync(identifier, testFile, new TestAsyncCallback());
                }
                else {
                    backend.write(identifier, testFile);
                }
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("file".equals(e.getMessage()));
            }
        }
        while (!async);
    }

    @Ignore
    @Test
    public void testBackendWriteRecordFileNotFoundThrowsException() throws IOException, NoSuchAlgorithmException {
        boolean async = true;
        do {
            async = !async;

            File testFile = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
            assertTrue(testFile.delete());
            try {
                if (async) {
                    backend.writeAsync(identifier, testFile, new TestAsyncCallback());
                }
                else {
                    backend.write(identifier, testFile);
                }
                fail();
            } catch (DataStoreException e) {
                assertTrue(e.getCause() instanceof FileNotFoundException);
            }
        }
        while (!async);
    }

    @Ignore
    @Test
    public void testBackendWriteAsyncNullCallbackThrowsNullPointerException() throws IOException, NoSuchAlgorithmException, DataStoreException {
        File testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 10), testFile);
        DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
        try {
            TestAsyncCallback callback = null;
            backend.writeAsync(identifier, testFile, callback);
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("callback".equals(e.getMessage()));
        }
    }

    // GetLength (Backend)

    @Ignore
    @Test
    public void testBackendGetLengthNullIdentifierThrowsNullPointerException() throws DataStoreException {
        DataIdentifier identifier = null;
        try {
            backend.getLength(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assert("identifier".equals(e.getMessage()));
        }
    }

    @Ignore
    @Test
    public void testBackendGetLengthInvalidIdentifierThrowsDataStoreException() {
        DataIdentifier identifier = new DataIdentifier("fake");
        try {
            backend.getLength(identifier);
            fail();
        }
        catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof StorageException);
        }
    }

    // LastModified (Backend)

    @Ignore
    @Test
    public void testBackendLastModifiedChangedOnUpdate() throws IOException, NoSuchAlgorithmException, DataStoreException{
        File testFile1 = folder.newFile();
        copyInputStreamToFile(randomStream(0, 10), testFile1);
        DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
        backend.write(identifier, testFile1);
        long timestamp1 = backend.getLastModified(identifier);

        File testFile2 = folder.newFile();
        copyInputStreamToFile(randomStream(1, 20), testFile2);
        backend.write(identifier, testFile2);
        long timestamp2 = backend.getLastModified(identifier);

        assertNotEquals(timestamp1, timestamp2);
    }

    @Ignore
    @Test
    public void testBackendLastModifiedNullIdentifierThrowsNullPointerException() throws DataStoreException {
        DataIdentifier identifier = null;
        try {
            backend.getLastModified(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assert("identifier".equals(e.getMessage()));
        }
    }

    @Ignore
    @Test
    public void testBackendLastModifiedInvalidIdentifierThrowsDataStoreException() {
        DataIdentifier identifier = new DataIdentifier("fake");
        try {
            backend.getLastModified(identifier);
            fail();
        }
        catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof StorageException);
        }
    }

    // Read (Backend)

    @Ignore
    @Test
    public void testBackendReadRecordNullIdentifier() throws DataStoreException {
        DataIdentifier identifier = null;
        try {
            backend.read(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assert("identifier".equals(e.getMessage()));
        }
    }

    @Ignore
    @Test
    public void testBackendReadRecordInvalidIdentifier() {
        DataIdentifier identifier = new DataIdentifier("fake");
        try {
            backend.read(identifier);
            fail();
        }
        catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof StorageException);
        }
    }

    // Delete (Backend)

    @Ignore
    @Test
    public void testBackendDeleteRecordNullIdentifier() throws DataStoreException {
        DataIdentifier identifier = null;
        try {
            backend.deleteRecord(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assert("identifier".equals(e.getMessage()));
        }
    }

    @Ignore
    @Test
    public void testBackendDeleteRecordInvalidIdentifier() {
        DataIdentifier identifier = new DataIdentifier("fake");
        try {
            backend.deleteRecord(identifier);
            fail();
        }
        catch (DataStoreException e) {
            assertTrue(e.getCause() instanceof StorageException);
        }
    }

    // DeleteAllOlderThan (Backend)

    @Ignore
    @Test
    public void testBackendDeleteAllOlderThanRemovesCorrectRecords() throws IOException, NoSuchAlgorithmException, DataStoreException {
        boolean deleteAll = true;
        boolean deleteNone = false;

        for (int i=0; i<3; i++) {
            File f1 = folder.newFile();
            File f2 = folder.newFile();
            copyInputStreamToFile(randomStream(1, 10), f1);
            copyInputStreamToFile(randomStream(2, 10), f2);
            DataIdentifier id1 = new DataIdentifier(getIdForInputStream(new FileInputStream(f1)));
            DataIdentifier id2 = new DataIdentifier(getIdForInputStream(new FileInputStream(f2)));

            long deleteNoneTime = DateTime.now().getMillis() - 5000; // Be sure it is before now
            backend.write(id1, f1);
            try { Thread.sleep(501); } catch (InterruptedException e) { }
            long deleteSomeTime = DateTime.now().getMillis();
            try { Thread.sleep(501); } catch (InterruptedException e) { }
            backend.write(id2, f2);
            long deleteAllTime = DateTime.now().getMillis() + 5000; // Be sure it is after now

            int deletedCount = 0;
            if (deleteAll) {
                deletedCount = backend.deleteAllOlderThan(deleteAllTime).size();
            }
            else if (deleteNone) {
                deletedCount = backend.deleteAllOlderThan(deleteNoneTime).size();
            }
            else {
                deletedCount = backend.deleteAllOlderThan(deleteSomeTime).size();
            }

            assertEquals(deleteAll ? 2 : (deleteNone ? 0 : 1), deletedCount);
            if (deleteAll) {
                deleteAll = false;
            }
            else {
                deleteNone = true;
            }

            backend.deleteAllOlderThan(deleteNoneTime);
        }
    }

    // Touch (Backend)

    @Ignore
    @Test
    public void testBackendTouchRecordUpdatesLastModified() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (boolean async : Lists.newArrayList(false, true)) {
            File testFile1 = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile1);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
            backend.write(identifier, testFile1);
            long lastModified = backend.getLastModified(identifier);
            long minModified = lastModified + 1001;

            if (async) {
                TestAsyncCallback callback = new TestAsyncCallback();
                backend.touchAsync(identifier, minModified, callback);
                callback.waitForTouchSuccess();
            }
            else {
                backend.touch(identifier, minModified);
            }

            assertEquals(minModified, backend.getLastModified(identifier));
        }
    }

    @Ignore
    @Test
    public void testBackendTouchMinModifiedOlderThanLastModifiedDoesNotUpdateLastModified() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (boolean async : Lists.newArrayList(false, true)) {
            File testFile1 = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile1);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
            backend.write(identifier, testFile1);
            long lastModified = backend.getLastModified(identifier);
            long minModified = lastModified - 1;

            if (async) {
                TestAsyncCallback callback = new TestAsyncCallback();
                backend.touchAsync(identifier, minModified, callback);
                callback.waitForTouchSuccess();
            }
            else {
                backend.touch(identifier, minModified);
            }

            assertEquals(lastModified, backend.getLastModified(identifier));
        }
    }

    @Ignore
    @Test
    public void testBackendTouchMinModifiedIsZeroDoesNotUpdateLastModified() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (boolean async : Lists.newArrayList(false, true)) {
            File testFile1 = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile1);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
            backend.write(identifier, testFile1);
            long lastModified = backend.getLastModified(identifier);

            if (async) {
                TestAsyncCallback callback = new TestAsyncCallback();
                backend.touchAsync(identifier, 0, callback);
                callback.waitForTouchSuccess();
            }
            else {
                backend.touch(identifier, 0);
            }

            assertEquals(lastModified, backend.getLastModified(identifier));
        }
    }

    @Ignore
    @Test
    public void testBackendTouchRecordDoesNotModifyData() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (boolean async : Lists.newArrayList(false, true)) {
            File testFile1 = folder.newFile();
            String testBuffer = "test";
            copyInputStreamToFile(new ByteArrayInputStream(testBuffer.getBytes()), testFile1);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
            backend.write(identifier, testFile1);
            long lastModified = backend.getLastModified(identifier);

            StringWriter writer1 = new StringWriter();
            IOUtils.copy(backend.read(identifier), writer1, "utf-8");
            assertTrue(testBuffer.equals(writer1.toString()));

            if (async) {
                TestAsyncCallback callback = new TestAsyncCallback();
                backend.touchAsync(identifier, lastModified + 1001, callback);
                callback.waitForTouchSuccess();
            }
            else {
                backend.touch(identifier, lastModified + 1001);
            }

            StringWriter writer2 = new StringWriter();
            IOUtils.copy(backend.read(identifier), writer2, "utf-8");
            assertTrue(testBuffer.equals(writer2.toString()));
        }
    }

    // Exists (Backend)

    @Ignore
    @Test
    public void testBackendNotCreatedRecordDoesNotExist() throws DataStoreException {
        assertFalse(backend.exists(new DataIdentifier(("fake"))));
    }

    @Ignore
    @Test
    public void testBackendRecordExistsNullIdentifierThrowsNullPointerException() throws DataStoreException {
        try {
            DataIdentifier nullIdentifier = null;
            backend.exists(nullIdentifier);
            fail();
        }
        catch (NullPointerException e) { }
    }

    @Ignore
    @Test
    public void testBackendRecordExistsDoesNotUpdateLastModified() throws DataStoreException, IOException, NoSuchAlgorithmException {
        File testFile1 = folder.newFile();
        String testBuffer = "test";
        copyInputStreamToFile(new ByteArrayInputStream(testBuffer.getBytes()), testFile1);
        DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
        backend.write(identifier, testFile1);

        long lastModified = backend.getLastModified(identifier);

        assertTrue(backend.exists(identifier));

        assertEquals(lastModified, backend.getLastModified(identifier));
    }

    @Ignore
    @Test
    public void testBackendRecordExistsWithTouchUpdatesLastModified() throws DataStoreException, IOException, NoSuchAlgorithmException {
        File testFile1 = folder.newFile();
        String testBuffer = "test";
        copyInputStreamToFile(new ByteArrayInputStream(testBuffer.getBytes()), testFile1);
        DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile1)));
        backend.write(identifier, testFile1);

        long lastModified = backend.getLastModified(identifier);

        backend.exists(identifier, true);

        assertNotEquals(lastModified, backend.getLastModified(identifier));
    }

    // GetAllIdentifiers (Backend)

    @Ignore
    @Test
    public void testBackendGetAllIdentifiersNoRecordsReturnsNone() throws DataStoreException {
        Iterator<DataIdentifier> allIdentifiers = backend.getAllIdentifiers();
        assertFalse(allIdentifiers.hasNext());
    }

    @Ignore
    @Test
    public void testBackendGetAllIdentifiers() throws DataStoreException, IOException, NoSuchAlgorithmException {
        for (int expectedRecCount : Lists.newArrayList(1, 2, 5)) {
            for (int i=0; i<expectedRecCount; i++) {
                File testfile = folder.newFile();
                copyInputStreamToFile(randomStream(i, 1024), testfile);
                DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testfile)));
                backend.write(identifier, testfile);
            }

            Iterator<DataIdentifier> allIdentifiersIter = backend.getAllIdentifiers();
            int actualRecCount = 0;
            while (allIdentifiersIter.hasNext()) {
                actualRecCount++;
                allIdentifiersIter.next();
            }

            backend.deleteAllOlderThan(DateTime.now().getMillis() + 10000);

            assertEquals(expectedRecCount, actualRecCount);
        }
    }

    // GetRecord (Backend)

    @Ignore
    @Test
    public void testBackendGetRecord() throws IOException, DataStoreException {
        String recordData = "testData";
        ds.resetSuccessfulUploads();
        DataRecord record = ds.addRecord(new ByteArrayInputStream(recordData.getBytes()));
        waitForUpload(ds);
        DataRecord retrievedRecord = backend.getRecord(record.getIdentifier());
        validateRecord(record, recordData, retrievedRecord);
    }

    @Ignore
    @Test
    public void testBackendGetRecordNullIdentifierThrowsNullPointerException() throws DataStoreException {
        try {
            DataIdentifier identifier = null;
            backend.getRecord(identifier);
            fail();
        }
        catch (NullPointerException e) {
            assertTrue("identifier".equals(e.getMessage()));
        }
    }

    @Ignore
    @Test
    public void testBackendGetRecordInvalidIdentifierThrowsDataStoreException() {
        try {
            backend.getRecord(new DataIdentifier("invalid"));
            fail();
        }
        catch (DataStoreException e) {

        }
    }

    // GetAllRecords (Backend)

    @Ignore
    @Test
    public void testBackendGetAllRecordsNoRecordsReturnsNone() {

    }

    @Ignore
    @Test
    public void testBackendGetAllRecordsAfterCreateReturnsOne() {

    }

    @Ignore
    @Test
    public void testBackendGetAllRecordsAfterUpdateReturnsOne() {

    }

    @Ignore
    @Test
    public void testBackendGetAllRecordsAfterMultipleWritesReturnsAll() {

    }

    // AddMetadataRecord (Backend)

    @Ignore
    @Test
    public void testBackendAddMetadataRecordFromInputStream() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordFromFile() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordInputStreamNotFound() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordFileNotFound() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordNullInputStream() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordNullFile() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordWithInputStreamNullName() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordWithInputStreamEmptyName() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordWithFileNullName() {

    }

    @Ignore
    @Test
    public void testBackendAddMetadataRecordWithFileEmptyName() {

    }

    // GetMetadataRecord (Backend)

    @Ignore
    @Test
    public void testBackendGetMetadataRecord() {

    }

    @Ignore
    @Test
    public void testBackendGetMetadataRecordNameDoesNotExist() {

    }

    @Ignore
    @Test
    public void testBackendGetMetadataRecordNullName() {

    }

    @Ignore
    @Test
    public void testBackendGetMetadataRecordEmptyName() {

    }

    // GetAllMetadataRecords (Backend)

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesAll() {

    }

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesSome() {

    }

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesOne() {

    }

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesNone() {

    }

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsNoRecordsReturnsNone() {

    }

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsNullPrefix() {

    }

    @Ignore
    @Test
    public void testBackendGetAllMetadataRecordsEmptyPrefix() {

    }

    // DeleteMetadataRecord (Backend)

    @Ignore
    @Test
    public void testBackendDeleteMetadataRecord() {

    }

    @Ignore
    @Test
    public void testBackendDeleteMetadataRecordNameDoesNotExist() {

    }

    @Ignore
    @Test
    public void testBackendDeleteMetadataRecordNullName() {

    }

    @Ignore
    @Test
    public void testBackendDeleteMetadataRecordEmptyName() {

    }

    // DeleteAllMetadataRecords (Backend)

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesAll() {

    }

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesSome() {

    }

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesOne() {

    }

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesNone() {

    }

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsNoRecordsNoChange() {

    }

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsNullPrefix() {

    }

    @Ignore
    @Test
    public void testBackendDeleteAllMetadataRecordsEmptyPrefix() {

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

    private static class TestAsyncCallback implements AsyncUploadCallback, AsyncTouchCallback {
        int uploadSuccesses = 0;
        int uploadFailures = 0;
        int uploadAborts = 0;
        int touchSuccesses = 0;
        int touchFailures = 0;
        int touchAborts = 0;

        @Override
        public void onSuccess(AsyncUploadResult result) { ++uploadSuccesses; }

        @Override
        public void onFailure(AsyncUploadResult result) { ++uploadFailures; }

        @Override
        public void onAbort(AsyncUploadResult result) { ++uploadAborts; }

        @Override
        public void onSuccess(AsyncTouchResult result) { ++touchSuccesses; }

        @Override
        public void onFailure(AsyncTouchResult result) { ++touchFailures; }

        @Override
        public void onAbort(AsyncTouchResult result) { ++touchAborts; }

        void reset() {
            uploadSuccesses = uploadFailures = uploadAborts = touchSuccesses = touchFailures = touchAborts = 0;
        }

        boolean waitForUploadSuccess() {
            for (int tries = 0; tries<10; ++tries) {
                if (uploadSuccesses > 0) return true;
                else if (uploadFailures > 0 || uploadAborts > 0) {
                    return false;
                }
                try { Thread.sleep(tries < 5 ? 100 : (tries < 8 ? 1000 : 2000)); } catch (InterruptedException e) { }
            }
            return false;
        }

        boolean waitForTouchSuccess() {
            for (int tries = 0; tries<10; ++tries) {
                if (touchSuccesses > 0) return true;
                else if (touchFailures > 0 || touchAborts > 0) {
                    return false;
                }
                try { Thread.sleep(tries < 5 ? 100 : (tries < 8 ? 1000 : 2000)); } catch (InterruptedException e) { }
            }
            return false;
        }
    }
}
