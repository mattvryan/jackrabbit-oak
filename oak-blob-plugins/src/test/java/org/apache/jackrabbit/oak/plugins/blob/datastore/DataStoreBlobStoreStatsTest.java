/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import javax.jcr.RepositoryException;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.stats.StatsCollectingStreams;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DataStoreBlobStoreStatsTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);
    private BlobStoreStats stats = new BlobStoreStats(statsProvider);

    private static int BLOB_LEN = 20*1024;

    @After
    public void shutDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testDSBSReadBlobStats() throws IOException, RepositoryException {
        DelayedReadDSBS dsbs = (DelayedReadDSBS) getDSBSBuilder().withReadDelay(1000).build();

        String blobId = dsbs.writeBlob(new RandomInputStream(System.currentTimeMillis(), BLOB_LEN));

        long downloadCount = stats.getDownloadCount();
        long downloadTotalSize = stats.getDownloadTotalSize();
        long downloadCountLastMinute = sum((long[]) stats.getDownloadCountHistory().get("per second"));
        long downloadAmountLastMinute = sum((long[]) stats.getDownloadSizeHistory().get("per second"));
        long downloadTimeLastMinute = sum((long[]) stats.getDownloadRateHistory().get("per second"));

        byte[] buffer = new byte[BLOB_LEN];
        dsbs.readBlob(blobId, 0, buffer, 0, BLOB_LEN);

        assertEquals(downloadCount + 1, stats.getDownloadCount());
        assertEquals(downloadTotalSize + BLOB_LEN, stats.getDownloadTotalSize());
        assertEquals(downloadCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getDownloadCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertEquals(downloadAmountLastMinute + BLOB_LEN,
                waitForMetric(input -> sum((long[])input.getDownloadSizeHistory().get("per second")),
                        stats, (long) BLOB_LEN, 0L).longValue());
        assertTrue(downloadTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getDownloadRateHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSReadBlobNotFoundStats() throws IOException, RepositoryException {
        // BLOB_DOWNLOAD_NOT_FOUND
    }

    @Test
    public void testDSBSReadBlobErrorStats() {
        // BLOB_DOWNLOAD_ERROS
    }

    @Test
    public void testDSBSWriteBlobStats() throws IOException, RepositoryException {
        DelayedWriteDSBS dsbs = (DelayedWriteDSBS) getDSBSBuilder().withWriteDelay(1000).build();

        long uploadCount = stats.getUploadCount();
        long uploadTotalSize = stats.getUploadTotalSize();
        long uploadCountLastMinute = sum((long[]) stats.getUploadCountHistory().get("per second"));
        long uploadAmountLastMinute = sum((long[]) stats.getUploadSizeHistory().get("per second"));
        long uploadTimeLastMinute = sum((long[]) stats.getUploadRateHistory().get("per second"));

        dsbs.writeBlob(new RandomInputStream(System.currentTimeMillis(), BLOB_LEN));

        assertEquals(uploadCount + 1, stats.getUploadCount());
        assertEquals(uploadTotalSize + BLOB_LEN, stats.getUploadTotalSize());
        assertEquals(uploadCountLastMinute + 1,
                waitForMetric(input -> sum((long[])input.getUploadCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertEquals(uploadAmountLastMinute + BLOB_LEN,
                waitForMetric(input -> sum((long[])input.getUploadSizeHistory().get("per second")),
                        stats, (long) BLOB_LEN, 0L).longValue());
        assertTrue(uploadTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getUploadRateHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSWriteBlobErrorStats() {
        // BLOB_UPLOAD_ERRORS
    }

    @Test
    public void testDSBSGetInputStreamStats() {
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetInputStreamNotFoundStats() {
        // BLOB_DOWNLOAD_NOT_FOUND
    }

    @Test
    public void testDSBSGetInputStreamErrorStats() {
        // BLOB_DOWNLOAD_ERRORS
    }

    @Test
    public void testDSBSAddRecordStats() {
        // BLOB_ADD_RECORD_COUNT, BLOB_ADD_RECORD_SIZE, BLOB_ADD_RECORD_TIME
    }

    @Test
    public void testDSBSAddRecordErrorStats() {
        // BLOB_ADD_RECORD_ERRORS
    }

    @Test
    public void testDSBSGetRecordStats() {
        // BLOB_GETREC_COUNT, BLOB_GETREC_TIME

        // Then read the stream from the rec and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetRecordNotFoundStats() {
        // BLOB_GETREC_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordErrorStats() {
        // BLOB_GETREC_ERRORS
    }

    @Test
    public void testDSBSGetRecordIfStoredStats() {
        // BLOB_GETRECIFSTORED_COUNT, BLOB_GETRECIFSTORED_TIME

        // Then read the stream from the rec and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetRecordIfStoredErrorStats() {
        // BLOB_GETRECIFSTORED_ERRORS
    }

    @Test
    public void testDSBSGetRecordByReferenceStats() {
        // BLOB_GETRECBYREF_COUNT, BLOB_GET_RECBYREF_TIME

        // Then read the stream from the rec and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetRecordByReferenceNotFoundStats() {
        // BLOB_GETRECBYREF_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordByReferenceErrorStats() {
        // BLOB_GETRECBYREF_ERRORS
    }

    @Test
    public void testDSBSGetRecordForIdStats() {
        // BLOB_GETRECFORID_COUNT, BLOB_GETRECFORID_TIME

        // Then read the stream from the rec and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetRecordForIdNotFoundStats() {
        // BLOB_GETRECFORID_NOT_FOUND
    }

    @Test
    public void testDSBSGetRecordForIdErrorStats() {
        // BLOB_GETRECFORID_ERRORS
    }

    @Test
    public void testDSBSGetAllRecordsStats() {
        // BLOB_GETALLRECS_COUNT, BLOB_GETALLRECS_TIME

        // Then read the stream from one of the recs and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetAllRecordsErrorStats() {
        // BLOB_GETALLRECS_ERRORS
    }

    @Test
    public void testDSBSDeleteRecordStats() throws Exception {
        // BLOB_DELETE_COUNT, BLOB_DELETE_TIME

        DelayedDeleteDSBS dsbs = (DelayedDeleteDSBS) getDSBSBuilder().withDeleteDelay(1010).build();
        DataRecord record = dsbs.addRecord(new RandomInputStream(System.currentTimeMillis(), BLOB_LEN));
        List<String> chunkIds = Lists.newArrayList(record.getIdentifier().toString());
        long modifiedBefore = Instant.now().plusSeconds(86400).toEpochMilli();

        long deleteCount = stats.getDeleteCount();
        long deleteCountLastMinute = sum((long[]) stats.getDeleteCountHistory().get("per second"));
        long deleteTimeLastMinute = sum((long[]) stats.getDeleteTimeHistory().get("per second"));

        assertTrue(idInDsbs(record.getIdentifier(), dsbs));
        assertTrue(dsbs.deleteChunks(chunkIds, modifiedBefore));
        assertFalse(idInDsbs(record.getIdentifier(), dsbs));

        assertEquals(deleteCount+1, stats.getDeleteCount());
        assertEquals(deleteCountLastMinute+1,
                waitForMetric(input -> sum((long[])input.getDeleteCountHistory().get("per second")),
                        stats, 1L, 0L).longValue());
        assertTrue(deleteTimeLastMinute <
                waitForNonzeroMetric(input -> sum((long[])input.getDeleteTimeHistory().get("per second")), stats));
    }

    @Test
    public void testDSBSDeleteRecordErrorStats() {
        // BLOB_DELETE_ERRORS
    }

    @Test
    public void testDSBSDeleteAllOlderThanStats() {
        // BLOB_DELETEBYDATE_COUNT, BLOB_DELETEBYDATE_TIME
    }

    @Test
    public void testDSBSDeleteAllOlderThanErrorStats() {
        // BLOB_DELETEBYDATE_ERRORS
    }

    @Test
    public void testDSBSListIdsStats() {
        // BLOB_LISTIDS_COUNT, BLOB_LISTIDS_TIME
    }

    @Test
    public void testDSBSListIdsErrorStats() {
        // BLOB_LISTIDS_ERRORS
    }

    @Test
    public void testDSBSAddMetaRecStats() {
        // BLOB_METADATA_ADD_COUNT, BLOB_METADATA_ADD_TIME, BLOB_METADATA_ADD_SIZE
    }

    @Test
    public void testDSBSAddMetaRecErrorStats() {
        // BLOB_METADATA_ADD_ERRORS
    }

    @Test
    public void testDSBSGetMetaRecStats() {
        // BLOB_METADATA_GET_COUNT, BLOB_METADATA_GET_TIME

        // Then read the stream from the rec and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetMetaRecNotFoundStats() {
        // BLOB_METADATA_GET_NOT_FOUND
    }

    @Test
    public void testDSBSGetMetaRecErrorStats() {
        // BLOB_METADATA_GET_ERRORS
    }

    @Test
    public void testDSBSGetAllMetaRecsStats() {
        // BLOB_METADATA_GETALL_COUNT, BLOB_METADATA_GETALL_TIME

        // Then read the stream from one of the recs and measure:
        // BLOB_DOWNLOAD_COUNT, BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME
    }

    @Test
    public void testDSBSGetAllMetaRecsErrorStats() {
        // BLOB_METADATA_GETALL_ERRORS
    }

    @Test
    public void testDSBSMetaRecExistsStats() {
        // BLOB_METADATA_EXISTS_COUNT, BLOB_METADATA_EXISTS_TIME
    }

    @Test
    public void testDSBSMetaRecExistsErrorStats() {
        // BLOB_METADATA_EXISTS_ERRORS
    }

    @Test
    public void testDSBSMetaDeleteStats() {
        // BLOB_METADATA_DELETE_COUNT, BLOB_METADATA_DELETE_TIME
    }

    @Test
    public void testDSBSMetaDeleteNotFoundStats() {
        // BLOB_METADATA_DELETE_NOT_FOUND
    }

    @Test
    public void testDSBSMetaDeleteErrorStats() {
        // BLOB_METADATA_DELETE_ERRORS
    }

    @Test
    public void testDSBSMetaDeleteAllStats() {
        // BLOB_METADATA_DELETEALL_COUNT, BLOB_METDATA_DELETEALL_TIME
    }

    @Test
    public void testDSBSMetaDeleteAllErrorStats() {
        // BLOB_METADATA_DELETEALL_ERRORS
    }

    @Test
    public void testDSBSInitUploadDBAStats() {
        // BLOB_DBA_UPLOAD_INIT_COUNT, BLOB_DBA_UPLOAD_INIT_TIME
    }

    @Test
    public void testDSBSInitUploadDBAErrorStats() {
        // BLOB_DBA_UPLOAD_INIT_ERRORS
    }

    @Test
    public void testDSBSCompleteUploadDBAStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_COUNT, BLOB_DBA_UPLOAD_COMPLETE_TIME
    }

    @Test
    public void testDSBSCompleteUploadDBAExistsStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_EXISTS
    }

    @Test
    public void testDSBSCompleteUploadDBANotFoundStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_NOT_FOUND
    }

    @Test
    public void testDSBSCompleteUploadDBAErrorStats() {
        // BLOB_DBA_UPLOAD_COMPLETE_ERRORS
    }

    @Test
    public void testDSBSDownloadGetUriDBAStats() {
        // BLOB_DBA_DOWNLOAD_GETURI_COUNT, BLOB_DBA_DOWNLOAD_GETURI_TIME
    }

    @Test
    public void testDSBSDownloadGetURIDBANotFoundStats() {
        // BLOB_DBA_DOWNLOAD_GETURI_NOT_FOUND
    }

    @Test
    public void testDSBSDownloadGetURIDBAErrorStats() {
        // BLOB_DBA_DOWNLOAD_GETURI_ERRORS
    }


    private DataStore setupDS() throws IOException, RepositoryException {
        DataStore ds = new OakFileDataStore();
        File homeDir = folder.newFolder();
        ds.init(homeDir.getAbsolutePath());
        return ds;
    }

    private long sum(long[] d) {
        long result = 0L;
        for (Long l : d) {
            result += l;
        }
        return result;
    }

    private <T, R> R waitForMetric(Function<T, R> f, T input, R expected, R defaultValue) {
        return waitForMetric(f, input, expected, defaultValue, 100, 1000);
    }

    private <T, R> R waitForMetric(Function<T, R> f, T input, R expected, R defaultValue, int intervalMilliseconds, int waitMilliseconds) {
        long end = System.currentTimeMillis() + waitMilliseconds;
        R output = f.apply(input);
        if (null != output && output.equals(expected)) {
            return output;
        }
        do {
            try {
                Thread.sleep(intervalMilliseconds);
            }
            catch (InterruptedException e) { }
            output = f.apply(input);
            if (null != output && output.equals(expected)){
                return output;
            }
        }
        while (System.currentTimeMillis() < end);
        return defaultValue;
    }

    private <T> Long waitForNonzeroMetric(Function<T, Long> f, T input) {
        return waitForNonzeroMetric(f, input, 100, 1000);
    }

    private <T> Long waitForNonzeroMetric(Function<T, Long> f, T input, int intervalMilliseconds, int waitMilliseconds) {
        long end = System.currentTimeMillis() + waitMilliseconds;
        Long output = f.apply(input);
        if (null != output && output > 0L) {
            return output;
        }
        do {
            try {
                Thread.sleep(intervalMilliseconds);
            }
            catch (InterruptedException e) { }
            output = f.apply(input);
            if (null != output && output > 0L) {
                return output;
            }
        }
        while (System.currentTimeMillis() < end);
        return 0L;
    }

    private boolean idInDsbs(DataIdentifier id, DataStoreBlobStore dsbs) throws DataStoreException {
        Iterator<DataIdentifier> iter = dsbs.getAllIdentifiers();
        while (iter.hasNext()) {
            if (iter.next().equals(id)) {
                return true;
            }
        }
        return false;
    }

    private DSBSBuilder getDSBSBuilder() throws IOException, RepositoryException {
        return new DSBSBuilder(setupDS(), stats);
    }

    private static class DSBSBuilder {
        private DataStore ds;
        private BlobStoreStats stats;

        private int readDelay = 0;
        private int writeDelay = 0;
        private int deleteDelay = 0;

        public DSBSBuilder(@NotNull DataStore ds, @NotNull BlobStoreStats stats) {
            this.ds = ds;
            this.stats = stats;
        }

        public DSBSBuilder withReadDelay(int delay) {
            readDelay = delay;
            return this;
        }

        public DSBSBuilder withWriteDelay(int delay) {
            writeDelay = delay;
            return this;
        }

        public DSBSBuilder withDeleteDelay(int delay) {
            deleteDelay = delay;
            return this;
        }

        private DataStoreBlobStore getDSBS() {
            if (0 < readDelay) {
                return new DelayedReadDSBS(ds, readDelay);
            }
            else if (0 < writeDelay) {
                return new DelayedWriteDSBS(ds, writeDelay);
            }
            else if (0 < deleteDelay) {
                return new DelayedDeleteDSBS(ds, deleteDelay);
            }
            return null;
        }

        public DataStoreBlobStore build() {
            DataStoreBlobStore dsbs = getDSBS();
            if (null != dsbs) {
                dsbs.setBlobStatsCollector(stats);
            }
            return dsbs;
        }
    }

    private static class DelayableDSBS extends DataStoreBlobStore {
        DelayableDSBS(DataStore ds) {
            super(ds);
        }

        InputStream getStreamDelayed(String blobId, int delay) throws IOException {
            try {
                InputStream in = getDataRecord(blobId).getStream();
                if (!(in instanceof BufferedInputStream)){
                    in = new BufferedInputStream(in);
                }
                InputStream result = StatsCollectingStreams.wrap(stats, blobId, in);
                try {
                    Thread.sleep(delay);
                }
                catch (InterruptedException e) { }
                return result;
            } catch (DataStoreException e) {
                throw new IOException(e);
            }
        }

        DataRecord writeStreamDelayed(InputStream is, BlobOptions opts, int delay) throws IOException, DataStoreException {
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) { }
            return super.writeStream(is, opts);
        }

        void deleteDelayed(DataIdentifier identifier, int delay) throws DataStoreException {
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) { }
            super.doDeleteRecord(identifier);
        }
    }

    private static class DelayedReadDSBS extends DelayableDSBS {
        private int readDelay;

        DelayedReadDSBS(DataStore ds, int readDelay) {
            super(ds);
            this.readDelay = readDelay;
        }

        @Override
        protected InputStream getStream(String blobId) throws IOException {
            return getStreamDelayed(blobId, readDelay);
        }
    }

    private static class DelayedWriteDSBS extends DelayableDSBS {
        private int writeDelay;

        DelayedWriteDSBS(DataStore ds, int writeDelay) {
            super(ds);
            this.writeDelay = writeDelay;
        }

        @Override
        protected DataRecord writeStream(InputStream is, BlobOptions opts) throws IOException, DataStoreException {
            return writeStreamDelayed(is, opts, writeDelay);
        }
    }

    private static class DelayedDeleteDSBS extends DelayableDSBS {
        private int deleteDelay;

        DelayedDeleteDSBS(DataStore ds, int deleteDelay) {
            super(ds);
            this.deleteDelay = deleteDelay;
        }

        @Override
        void doDeleteRecord(DataIdentifier identifier) throws DataStoreException {
            deleteDelayed(identifier, deleteDelay);
        }
    }
}
