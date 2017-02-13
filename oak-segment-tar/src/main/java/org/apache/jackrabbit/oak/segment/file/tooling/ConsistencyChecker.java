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

package org.apache.jackrabbit.oak.segment.file.tooling;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Math.min;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for checking the files of a
 * {@link FileStore} for inconsistency and
 * reporting that latest consistent revision.
 */
public class ConsistencyChecker implements Closeable {

    private static class StatisticsIOMonitor extends IOMonitorAdapter {

        private final AtomicLong ioOperations = new AtomicLong(0);

        private final AtomicLong bytesRead = new AtomicLong(0);

        @Override
        public void beforeSegmentRead(File file, long msb, long lsb, int length) {
            ioOperations.incrementAndGet();
            bytesRead.addAndGet(length);
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(ConsistencyChecker.class);

    private final StatisticsIOMonitor statisticsIOMonitor = new StatisticsIOMonitor();

    private final ReadOnlyFileStore store;

    private final long debugInterval;

    /**
     * Run a consistency check.
     *
     * @param directory  directory containing the tar files
     * @param journalFileName  name of the journal file containing the revision history
     * @param fullTraversal    full traversal consistency check if {@code true}. Only try
     *                         to access the root node otherwise.
     * @param debugInterval    number of seconds between printing progress information to
     *                         the console during the full traversal phase.
     * @param binLen           number of bytes to read from binary properties. -1 for all.
     * @throws IOException
     */
    public static void checkConsistency(
            File directory,
            String journalFileName,
            boolean fullTraversal,
            long debugInterval,
            long binLen,
            boolean ioStatistics
    ) throws IOException, InvalidFileStoreVersionException {
        print("Searching for last good revision in {}", journalFileName);
        try (
                JournalReader journal = new JournalReader(new File(directory, journalFileName));
                ConsistencyChecker checker = new ConsistencyChecker(directory, debugInterval, ioStatistics)
        ) {
            Set<String> badPaths = newHashSet();
            String latestGoodRevision = null;
            int revisionCount = 0;

            while (journal.hasNext() && latestGoodRevision == null) {
                String revision = journal.next();
                try {
                    print("Checking revision {}", revision);
                    revisionCount++;
                    String badPath = checker.check(revision, badPaths, binLen);
                    if (badPath == null && fullTraversal) {
                        badPath = checker.traverse(revision, binLen);
                    }
                    if (badPath == null) {
                        print("Found latest good revision {}", revision);
                        print("Searched through {} revisions", revisionCount);
                        latestGoodRevision = revision;
                    } else {
                        badPaths.add(badPath);
                        print("Broken revision {}", revision);
                    }
                } catch (IllegalArgumentException e) {
                    print("Skipping invalid record id {}", revision);
                }
            }

            if (ioStatistics) {
                print(
                        "[I/O] Segment read operations: {}",
                        checker.statisticsIOMonitor.ioOperations
                );
                print(
                        "[I/O] Segment bytes read: {} ({} bytes)",
                        humanReadableByteCount(checker.statisticsIOMonitor.bytesRead.get()),
                        checker.statisticsIOMonitor.bytesRead
                );
            }

            if (latestGoodRevision == null) {
                print("No good revision found");
            }
        }
    }

    /**
     * Create a new consistency checker instance
     *
     * @param directory  directory containing the tar files
     * @param debugInterval    number of seconds between printing progress information to
     *                         the console during the full traversal phase.
     * @throws IOException
     */
    public ConsistencyChecker(File directory, long debugInterval, boolean ioStatistics) throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder builder = fileStoreBuilder(directory);
        if (ioStatistics) {
            builder.withIOMonitor(statisticsIOMonitor);
        }
        this.store = builder.buildReadOnly();
        this.debugInterval = debugInterval;
    }

    /**
     * Check whether the nodes and all its properties of all given
     * {@code paths} are consistent at the given {@code revision}.
     *
     * @param revision  revision to check
     * @param paths     paths to check
     * @param binLen    number of bytes to read from binary properties. -1 for all.
     * @return  Path of the first inconsistency detected or {@code null} if none.
     */
    public String check(String revision, Set<String> paths, long binLen) {
        store.setRevision(revision);
        for (String path : paths) {
            String err = checkPath(path, binLen);
            if (err != null) {
                return err;
            }
        }
        return null;
    }

    private String checkPath(String path, long binLen) {
        try {
            print("Checking {}", path);
            NodeState root = SegmentNodeStoreBuilders.builder(store).build().getRoot();
            String parentPath = getParentPath(path);
            String name = getName(path);
            NodeState parent = getNode(root, parentPath);
            if (!denotesRoot(path) && parent.hasChildNode(name)) {
                return traverse(parent.getChildNode(name), path, false, binLen);
            } else {
                return traverse(parent, parentPath, false, binLen);
            }
        } catch (RuntimeException e) {
            print("Error while checking {}: {}", path, e.getMessage());
            return path;
        }
    }

    private int nodeCount;
    private int propertyCount;

    /**
     * Travers the given {@code revision}
     * @param revision  revision to travers
     * @param binLen    number of bytes to read from binary properties. -1 for all.
     */
    public String traverse(String revision, long binLen) {
        try {
            store.setRevision(revision);
            nodeCount = 0;
            propertyCount = 0;
            String result = traverse(SegmentNodeStoreBuilders.builder(store).build()
                    .getRoot(), "/", true, binLen);
            print("Checked {} nodes and {} properties", nodeCount, propertyCount);
            return result;
        } catch (RuntimeException e) {
            print("Error while traversing {}", revision, e.getMessage());
            return "/";
        }
    }

    private String traverse(NodeState node, String path, boolean deep, long binLen) {
        try {
            debug("Traversing {}", path);
            nodeCount++;
            for (PropertyState propertyState : node.getProperties()) {
                debug("Checking {}/{}", path, propertyState);
                Type<?> type = propertyState.getType();
                if (type == BINARY) {
                    traverse(propertyState.getValue(BINARY), binLen);
                } else if (type == BINARIES) {
                    for (Blob blob : propertyState.getValue(BINARIES)) {
                        traverse(blob, binLen);
                    }
                } else {
                    propertyCount++;
                    propertyState.getValue(type);
                }
            }
            for (ChildNodeEntry cne : node.getChildNodeEntries()) {
                String childName = cne.getName();
                NodeState child = cne.getNodeState();
                if (deep) {
                    String result = traverse(child, concat(path, childName), true, binLen);
                    if (result != null) {
                        return result;
                    }
                }
            }
            return null;
        } catch (RuntimeException | IOException e) {
            print("Error while traversing {}: {}", path, e.getMessage());
            return path;
        }
    }

    private void traverse(Blob blob, long length) throws IOException {
        if (length < 0) {
            length = Long.MAX_VALUE;
        }
        if (length > 0 && !isExternal(blob)) {
            InputStream s = blob.getNewStream();
            try {
                byte[] buffer = new byte[8192];
                int l = s.read(buffer, 0, (int) min(buffer.length, length));
                while (l >= 0 && (length -= l) > 0) {
                    l = s.read(buffer, 0, (int) min(buffer.length, length));
                }
            } finally {
                s.close();
            }
            
            propertyCount++;
        }
    }

    private static boolean isExternal(Blob b) {
        if (b instanceof SegmentBlob) {
            return ((SegmentBlob) b).isExternal();
        }
        return false;
    }

    @Override
    public void close() {
        store.close();
    }

    private static void print(String format) {
        LOG.info(format);
    }

    private static void print(String format, Object arg) {
        LOG.info(format, arg);
    }

    private static void print(String format, Object arg1, Object arg2) {
        LOG.info(format, arg1, arg2);
    }

    private long ts;

    private void debug(String format, Object arg) {
        if (debug()) {
            LOG.debug(format, arg);
        }
    }

    private void debug(String format, Object arg1, Object arg2) {
        if (debug()) {
            LOG.debug(format, arg1, arg2);
        }
    }

    private boolean debug() {
        // Avoid calling System.currentTimeMillis(), which is slow on some systems.
        if (debugInterval == Long.MAX_VALUE) {
            return false;
        } else if (debugInterval == 0) {
            return true;
        }

        long ts = System.currentTimeMillis();
        if ((ts - this.ts) / 1000 > debugInterval) {
            this.ts = ts;
            return true;
        } else {
            return false;
        }
    }


}
