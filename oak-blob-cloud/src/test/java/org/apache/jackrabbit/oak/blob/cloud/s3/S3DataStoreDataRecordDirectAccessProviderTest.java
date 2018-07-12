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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import static java.lang.System.getProperty;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.AbstractDataRecordDirectAccessProviderTest;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordDirectAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDirectUploadException;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class S3DataStoreDataRecordDirectAccessProviderTest extends AbstractDataRecordDirectAccessProviderTest {
    @ClassRule
    public static TemporaryFolder homeDir = new TemporaryFolder(new File("target"));

    private static S3DataStore dataStore;

    @BeforeClass
    public static void setupDataStore() throws Exception {
        dataStore = (S3DataStore) S3DataStoreUtils.getS3DataStore(
                "org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore",
                getProperties("s3.config",
                        "aws.properties",
                        ".aws"),
                homeDir.newFolder().getAbsolutePath()
        );
        dataStore.setHttpDownloadURLExpirySeconds(expirySeconds);
        dataStore.setHttpUploadURLExpirySeconds(expirySeconds);
    }

    @Override
    protected ConfigurableDataRecordDirectAccessProvider getDataStore() {
        return dataStore;
    }

    @Override
    protected DataRecord doGetRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        return ds.getRecord(identifier);
    }

    @Override
    protected DataRecord doSynchronousAddRecord(DataStore ds, InputStream in) throws DataStoreException {
        return ((S3DataStore)ds).addRecord(in, new BlobOptions().setUpload(BlobOptions.UploadType.SYNCHRONOUS));
    }

    @Override
    protected void doDeleteRecord(DataStore ds, DataIdentifier identifier) throws DataStoreException {
        ((S3DataStore)ds).deleteRecord(identifier);
    }

    @Override
    protected long getProviderMinPartSize() {
        return Math.max(0L, S3DataStore.minPartSize);
    }

    @Override
    protected long getProviderMaxPartSize() {
        return S3DataStore.maxPartSize;
    }

    @Override
    protected long getProviderMaxSinglePutSize() { return S3DataStore.maxSinglePutUploadSize; }

    @Override
    protected long getProviderMaxBinaryUploadSize() { return S3DataStore.maxBinaryUploadSize; }

    @Override
    protected boolean isSinglePutURL(URL url) {
        Map<String, String> queryParams = parseQueryString(url);
        return ! queryParams.containsKey(S3Backend.PART_NUMBER) && ! queryParams.containsKey(S3Backend.UPLOAD_ID);
    }

    @Override
    protected HttpsURLConnection getHttpsConnection(long length, URL url) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now()));
        conn.setRequestProperty("Host", url.getHost());

        return conn;
    }

    /** Only run if explicitly asked to via -Dtest=S3DataStoreDataRecordDirectAccessProviderTest */
    /** Run like this:  mvn test -Dtest=S3DataStoreDataRecordDirectAccessProviderTest -Dtest.opts.memory=-Xmx2G */
    private static final boolean INTEGRATION_TESTS_ENABLED =
            S3DataStoreDataRecordDirectAccessProviderTest.class.getSimpleName().equals(getProperty("test"));
    @Override
    protected boolean integrationTestsEnabled() {
        return INTEGRATION_TESTS_ENABLED;
    }

    @Test
    public void testInitDirectUploadURLHonorsExpiryTime() throws DataRecordDirectUploadException {
        ConfigurableDataRecordDirectAccessProvider ds = getDataStore();
        try {
            ds.setHttpUploadURLExpirySeconds(60);
            DataRecordDirectUpload uploadContext = ds.initiateHttpUpload(ONE_MB, 1);
            URL uploadUrl = uploadContext.getUploadURLs().iterator().next();
            Map<String, String> params = parseQueryString(uploadUrl);
            String expiresTime = params.get("X-Amz-Expires");
            assertTrue(60 >= Integer.parseInt(expiresTime));
        }
        finally {
            ds.setHttpUploadURLExpirySeconds(expirySeconds);
        }
    }

    @Test
    public void testInitiateHttpUploadUnlimitedURLs() throws DataRecordDirectUploadException {
        ConfigurableDataRecordDirectAccessProvider ds = getDataStore();
        long uploadSize = ONE_GB * 50;
        int expectedNumUrls = 5000;
        DataRecordDirectUpload upload = ds.initiateHttpUpload(uploadSize, -1);
        assertEquals(expectedNumUrls, upload.getUploadURLs().size());

        uploadSize = ONE_GB * 100;
        expectedNumUrls = 10000;
        upload = ds.initiateHttpUpload(uploadSize, -1);
        assertEquals(expectedNumUrls, upload.getUploadURLs().size());

        uploadSize = ONE_GB * 200;
        // expectedNumUrls still 10000, AWS limit
        upload = ds.initiateHttpUpload(uploadSize, -1);
        assertEquals(expectedNumUrls, upload.getUploadURLs().size());
    }
}
