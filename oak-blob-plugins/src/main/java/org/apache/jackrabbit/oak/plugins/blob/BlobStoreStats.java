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

package org.apache.jackrabbit.oak.plugins.blob;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.stats.TimeSeriesAverage;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class BlobStoreStats extends AnnotatedStandardMBean implements BlobStoreStatsMBean, BlobStatsCollector {
    private final Logger opsLogger = LoggerFactory.getLogger("org.apache.jackrabbit.oak.operations.blobs");
    private static final String BLOB_UPLOADS = "BLOB_UPLOADS";
    private static final String BLOB_UPLOAD_COUNT = "BLOB_UPLOAD_COUNT";
    private static final String BLOB_UPLOAD_SIZE = "BLOB_UPLOAD_SIZE";
    private static final String BLOB_UPLOAD_TIME = "BLOB_UPLOAD_TIME";
    private static final String BLOB_UPLOAD_ERROR_COUNT = "BLOB_UPLOAD_ERROR_COUNT";

    private static final String BLOB_DOWNLOADS = "BLOB_DOWNLOADS";
    private static final String BLOB_DOWNLOAD_COUNT = "BLOB_DOWNLOAD_COUNT";
    private static final String BLOB_DOWNLOAD_SIZE = "BLOB_DOWNLOAD_SIZE";
    private static final String BLOB_DOWNLOAD_TIME = "BLOB_DOWNLOAD_TIME";

    private static final String BLOB_DELETE_COUNT = "BLOB_DELETE_COUNT";
    private static final String BLOB_DELETE_TIME = "BLOB_DELETE_TIME";
    private static final String BLOB_ADD_RECORD_COUNT = "BLOB_ADD_RECORD_COUNT";
    private static final String BLOB_ADD_RECORD_SIZE = "BLOB_ADD_RECORD_SIZE";
    private static final String BLOB_ADD_RECORD_TIME = "BLOB_ADD_RECORD_TIME";
    private static final String BLOB_ADD_RECORD_ERROR_COUNT = "BLOB_ADD_RECORD_ERROR_COUNT";

    private static final String BLOB_GET_RECORD_COUNT = "BLOB_GET_RECORD_COUNT";
    private static final String BLOB_GET_RECORD_TIME = "BLOB_GET_RECORD_TIME";
    private static final String BLOB_GET_RECORD_ERROR_COUNT = "BLOB_GET_RECORD_ERROR_COUNT";


    private final StatisticsProvider statisticsProvider;

    private final HistogramStats uploadHisto;
    private final MeterStats uploadCount;
    private final MeterStats uploadErrorCount;
    private final MeterStats uploadSizeSeries;
    private final MeterStats uploadTimeSeries;
    private final TimeSeries uploadRateSeries;

    private final HistogramStats downloadHisto;
    private final MeterStats downloadCount;
    private final MeterStats downloadSizeSeries;
    private final MeterStats downloadTimeSeries;
    private final TimeSeries downloadRateSeries;

    private final MeterStats deleteCount;
    private final MeterStats deleteTimeSeries;

    private final MeterStats addRecordCount;
    private final MeterStats addRecordSizeSeries;
    private final MeterStats addRecordTimeSeries;
    private final TimeSeries addRecordRateSeries;
    private final MeterStats addRecordErrorCount;

    private final MeterStats getRecordCount;
    private final MeterStats getRecordTimeSeries;
    private final MeterStats getRecordErrorCount;

    private final TimeUnit recordedTimeUnit = TimeUnit.NANOSECONDS;

    public BlobStoreStats(@NotNull  StatisticsProvider sp) {
        super(BlobStoreStatsMBean.class);
        this.statisticsProvider = checkNotNull(sp);

        this.uploadHisto = sp.getHistogram(BLOB_UPLOADS, StatsOptions.DEFAULT);
        this.uploadCount = sp.getMeter(BLOB_UPLOAD_COUNT, StatsOptions.DEFAULT);
        this.uploadErrorCount = sp.getMeter(BLOB_UPLOAD_ERROR_COUNT, StatsOptions.DEFAULT);
        this.uploadSizeSeries = sp.getMeter(BLOB_UPLOAD_SIZE, StatsOptions.TIME_SERIES_ONLY);
        this.uploadTimeSeries = sp.getMeter(BLOB_UPLOAD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.uploadRateSeries = getAvgTimeSeries(BLOB_UPLOAD_SIZE, BLOB_UPLOAD_TIME);

        this.downloadHisto = sp.getHistogram(BLOB_DOWNLOADS, StatsOptions.DEFAULT);
        this.downloadCount = sp.getMeter(BLOB_DOWNLOAD_COUNT, StatsOptions.DEFAULT);
        this.downloadSizeSeries = sp.getMeter(BLOB_DOWNLOAD_SIZE, StatsOptions.TIME_SERIES_ONLY);
        this.downloadTimeSeries = sp.getMeter(BLOB_DOWNLOAD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.downloadRateSeries = getAvgTimeSeries(BLOB_DOWNLOAD_SIZE, BLOB_DOWNLOAD_TIME);

        this.deleteCount = sp.getMeter(BLOB_DELETE_COUNT, StatsOptions.DEFAULT);
        this.deleteTimeSeries = sp.getMeter(BLOB_DELETE_TIME, StatsOptions.TIME_SERIES_ONLY);

        this.addRecordCount = sp.getMeter(BLOB_ADD_RECORD_COUNT, StatsOptions.DEFAULT);
        this.addRecordSizeSeries = sp.getMeter(BLOB_ADD_RECORD_SIZE, StatsOptions.TIME_SERIES_ONLY);
        this.addRecordTimeSeries = sp.getMeter(BLOB_ADD_RECORD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.addRecordRateSeries = getAvgTimeSeries(BLOB_ADD_RECORD_SIZE, BLOB_ADD_RECORD_TIME);
        this.addRecordErrorCount = sp.getMeter(BLOB_ADD_RECORD_ERROR_COUNT, StatsOptions.DEFAULT);

        this.getRecordCount = sp.getMeter(BLOB_GET_RECORD_COUNT, StatsOptions.DEFAULT);
        this.getRecordTimeSeries = sp.getMeter(BLOB_GET_RECORD_TIME, StatsOptions.TIME_SERIES_ONLY);
        this.getRecordErrorCount = sp.getMeter(BLOB_GET_RECORD_ERROR_COUNT, StatsOptions.DEFAULT);
    }

    @Override
    public void uploaded(long timeTaken, TimeUnit unit, long size) {
        uploadHisto.update(size);

        //Recording upload like this is not accurate. A more accurate way
        //would be to mark as upload or download is progressing.
        //That would however add quite a bit of overhead
        //Approach below would record an upload/download at moment when
        //it got completed. So acts like a rough approximation
        uploadSizeSeries.mark(size);
        uploadTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Uploaded {} bytes in {} ms", size, unit.toMillis(timeTaken));
    }

    @Override
    public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {
        downloadHisto.update(size);

        downloadSizeSeries.mark(size);
        downloadTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Downloaded {} - {} bytes in {} ms", blobId, size, unit.toMillis(timeTaken));
    }

    @Override
    public void uploadCompleted(String blobId) {
        uploadCount.mark();
        opsLogger.debug("Upload completed - {}", blobId);
    }

    @Override
    public void downloadCompleted(String blobId) {
        downloadCount.mark();
        opsLogger.debug("Download completed - {}", blobId);
    }

    @Override
    public void uploadFailed() {
        uploadErrorCount.mark();
    }

    @Override
    public void deleted(String blobId, long timeTaken, TimeUnit unit) {
        deleteTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Deleted {} in {} ms", blobId, unit.toMillis(timeTaken));
    }

    @Override
    public void deleteCompleted(String blobId) {
        deleteCount.mark();
        opsLogger.debug("Delete completed - {}", blobId);
    }

    @Override
    public void recordAdded(long timeTaken, TimeUnit unit, long size) {
        addRecordSizeSeries.mark(size);
        addRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Record added - {} bytes in {} ms", size, unit.toMillis(timeTaken));
    }

    @Override
    public void addRecordCompleted(String blobId) {
        addRecordCount.mark();
        opsLogger.debug("Add record completed - {}", blobId);
    }

    @Override
    public void addRecordFailed() {
        addRecordErrorCount.mark();
        opsLogger.debug("Add record failed");
    }

    @Override
    public void getRecordCalled(long timeTaken, TimeUnit unit) {
        getRecordTimeSeries.mark(recordedTimeUnit.convert(timeTaken, unit));
        opsLogger.debug("Get record called - {} ms", unit.toMillis(timeTaken));
    }

    @Override
    public void getRecordCompleted(String blobId) {
        getRecordCount.mark();
        opsLogger.debug("Get record completed - {}", blobId);
    }

    @Override
    public void getRecordFailed(String blobId) {
        getRecordErrorCount.mark();
        opsLogger.debug("Get record failed - {}", blobId);
    }

    //~--------------------------------------< BlobStoreMBean >

    @Override
    public long getUploadTotalSize() {
        return uploadSizeSeries.getCount();
    }

    @Override
    public long getUploadCount() {
        return uploadCount.getCount();
    }

    @Override
    public long getUploadTotalSeconds() {
        return recordedTimeUnit.toSeconds(uploadTimeSeries.getCount());
    }

    @Override
    public long getUploadErrorCount() { return uploadErrorCount.getCount(); }

    @Override
    public long getDownloadTotalSize() {
        return downloadSizeSeries.getCount();
    }

    @Override
    public long getDownloadCount() {
        return downloadCount.getCount();
    }

    @Override
    public long getDownloadTotalSeconds() {
        return recordedTimeUnit.toSeconds(downloadTimeSeries.getCount());
    }

    @Override
    public long getAddRecordTotalSize() { return addRecordSizeSeries.getCount(); }

    @Override
    public long getAddRecordCount() { return addRecordCount.getCount(); }

    @Override
    public long getGetRecordCount() { return getRecordCount.getCount(); }

    @Override
    public long getGetRecordErrorCount() { return getRecordErrorCount.getCount(); }

    @Override
    public String blobStoreInfoAsString() {
        return String.format("Uploads - size = %s, count = %d%nDownloads - size = %s, count = %d",
                humanReadableByteCount(getUploadTotalSize()),
                getUploadCount(),
                humanReadableByteCount(getDownloadTotalSize()),
                getDownloadCount()
        );
    }

    @Override
    public CompositeData getUploadSizeHistory() {
        return getTimeSeriesData(BLOB_UPLOADS, "Blob Uploads (bytes)");
    }

    @Override
    public CompositeData getDownloadSizeHistory() {
        return getTimeSeriesData(BLOB_DOWNLOADS, "Blob Downloads (bytes)");
    }

    @Override
    public CompositeData getUploadRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(uploadRateSeries, "Blob uploads bytes/secs");
    }

    @Override
    public CompositeData getDownloadRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(downloadRateSeries, "Blob downloads bytes/secs");
    }

    @Override
    public CompositeData getUploadCountHistory() {
        return getTimeSeriesData(BLOB_UPLOAD_COUNT, "Blob Upload Counts");
    }

    @Override
    public CompositeData getDownloadCountHistory() {
        return getTimeSeriesData(BLOB_DOWNLOAD_COUNT, "Blob Download Counts");
    }

    @Override
    public long getDeleteCount() {
        return deleteCount.getCount();
    }

    @Override
    public CompositeData getDeleteCountHistory() {
        return getTimeSeriesData(BLOB_DELETE_COUNT, "Blob Delete Counts");
    }

    @Override
    public CompositeData getDeleteTimeHistory() {
        return getTimeSeriesData(BLOB_DELETE_TIME, "Blob record deletes/sec");
    }

    @Override
    public CompositeData getAddRecordCountHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_COUNT, "Blob Add Record Counts"); }

    @Override
    public CompositeData getAddRecordSizeHistory() { return getTimeSeriesData(BLOB_ADD_RECORD_SIZE, "Blob Add Record (bytes)"); }

    @Override
    public CompositeData getAddRecordRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(addRecordRateSeries, "Blob Add Record bytes/secs");
    }

    @Override
    public CompositeData getGetRecordCountHistory() { return getTimeSeriesData(BLOB_GET_RECORD_COUNT, "Blob Get Record Counts"); }

    @Override
    public CompositeData getGetRecordTimeHistory() { return getTimeSeriesData(BLOB_GET_RECORD_TIME, "Blob Get Record per second"); }

    @Override
    public long getAddRecordErrorCount() { return addRecordErrorCount.getCount(); }

    private CompositeData getTimeSeriesData(String name, String desc){
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }

    private TimeSeries getAvgTimeSeries(String nameValue, String nameCounter){
        return new TimeSeriesAverage(getTimeSeries(nameValue),
                new UnitConvertingTimeSeries(getTimeSeries(nameCounter), recordedTimeUnit, TimeUnit.SECONDS));
    }

    /**
     * TimeSeries which converts a Nanonsecond based time to Seconds for
     * calculating bytes/sec rate for upload and download
     */
    private static class UnitConvertingTimeSeries implements TimeSeries {
        private final TimeSeries series;
        private final TimeUnit source;
        private final TimeUnit dest;

        public UnitConvertingTimeSeries(TimeSeries series, TimeUnit source, TimeUnit dest) {
            this.series = series;
            this.source = source;
            this.dest = dest;
        }

        @Override
        public long[] getValuePerSecond() {
            return convert(series.getValuePerSecond());
        }

        @Override
        public long[] getValuePerMinute() {
            return convert(series.getValuePerMinute());
        }

        @Override
        public long[] getValuePerHour() {
            return convert(series.getValuePerHour());
        }

        @Override
        public long[] getValuePerWeek() {
            return convert(series.getValuePerWeek());
        }

        @Override
        public long getMissingValue() {
            return 0;
        }

        private long[] convert(long[] timings){
            for (int i = 0; i < timings.length; i++) {
                timings[i] = dest.convert(timings[i], source);
            }
            return timings;
        }
    }
}
