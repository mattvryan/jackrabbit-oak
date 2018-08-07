/**************************************************************************
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
 *
 *************************************************************************/

package org.apache.jackrabbit.oak.jcr.binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.jcr.Binary;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.FileDataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.SegmentMemoryNodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils;
import org.apache.jackrabbit.oak.jcr.binary.util.Content;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This is a unit test for the direct binary access JCR API extension.
 * It uses a mock of the underlying BlobAccessProvider.
 *
 * For a full integration test against real binary cloud storage,
 * see BinaryAccessIt.
 */
@RunWith(Parameterized.class)
public class BinaryAccessTest extends AbstractRepositoryTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        Collection<NodeStoreFixture> fixtures = new ArrayList<>();
        fixtures.add(new SegmentMemoryNodeStoreFixture(new FileDataStoreFixture()));
        return fixtures;
    }

    public BinaryAccessTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    private static final String FILE_PATH = "/file";

    private static final long SEGMENT_INLINE_SIZE = 16 * 1024;

    private static final String DOWNLOAD_URL = "http://expected.com/dummy/url/for/test/download";

    private static URI expectedDownloadURI() {
        return toURI(DOWNLOAD_URL);
    }

    private static final String UPLOAD_TOKEN = "super-safe-encrypted-token";

    private static final String UPLOAD_URL = "http://expected.com/dummy/url/for/test/upload";

    private static URI expectedUploadURI() {
        return toURI(UPLOAD_URL);
    }

    private static URI toURI(String url) {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    protected Content blobContent;

    /**
     * Adjust JCR repository creation to register a mock BlobAccessProvider in Whiteboard
     * so it can be picked up by oak-jcr.
     */
    @Override
    protected Repository createRepository(NodeStore nodeStore) {
        Whiteboard wb = new DefaultWhiteboard();
        wb.register(BlobAccessProvider.class, new MockBlobAccessProvider(), Collections.emptyMap());
        return initJcr(new Jcr(nodeStore).with(wb)).createRepository();
    }

    private class MockBlobAccessProvider implements BlobAccessProvider {

        @Override
        public @Nullable BlobUpload initiateBlobUpload(long maxSize, int maxURIs) throws IllegalArgumentException {
            return new BlobUpload() {
                @Override
                public @NotNull String getUploadToken() {
                    return UPLOAD_TOKEN;
                }

                @Override
                public long getMinPartSize() {
                    return 0;
                }

                @Override
                public long getMaxPartSize() {
                    return 10 * 1024 * 1024;
                }

                @Override
                public @NotNull Collection<URI> getUploadURIs() {
                    Collection<URI> uris = new ArrayList<>();
                    uris.add(expectedUploadURI());
                    return uris;
                }
            };
        }

        @Override
        public @Nullable Blob completeBlobUpload(@NotNull String uploadToken) throws IllegalArgumentException {
            if (!UPLOAD_TOKEN.equals(uploadToken)) {
                return null;
            }

            // this returns the binary content set on the "blobContent" member
            // as a simple way to mock some binary "storage"
            return new Blob() {

                @Override
                public @NotNull InputStream getNewStream() {
                    return blobContent.getStream();
                }

                @Override
                public long length() {
                    return blobContent.size();
                }

                @Override
                public String getReference() {
                    return "super-secure-key#" + getContentIdentity();
                }

                @Override
                public String getContentIdentity() {
                    return DigestUtils.md5Hex(blobContent.toString());
                }
            };
        }

        @Override
        public @Nullable URI getDownloadURI(@NotNull Blob blob,
                                            @NotNull BlobDownloadOptions blobDownloadOptions) {
            return expectedDownloadURI();
        }
    }

    @Test
    public void testBinaryDownload() throws RepositoryException {
        Content content = Content.createRandom(SEGMENT_INLINE_SIZE * 2);
        Binary binary = BinaryAccessTestUtils.storeBinaryAndRetrieve(getAdminSession(), FILE_PATH, content);

        assertTrue(binary instanceof BinaryDownload);

        BinaryDownload binaryDownload = (BinaryDownload) binary;
        URI uri = binaryDownload.getURI(BinaryDownloadOptions.DEFAULT);

        // we only need test that the we get a URI back (from our mock) to validate oak-jcr's inner workings
        assertNotNull(uri);
        assertEquals(expectedDownloadURI(), uri);
    }

    @Test
    public void testBinaryUpload() throws RepositoryException, IOException {
        Content content = Content.createRandom(SEGMENT_INLINE_SIZE * 2);

        ValueFactory vf = getAdminSession().getValueFactory();
        assertTrue(vf instanceof JackrabbitValueFactory);

        JackrabbitValueFactory valueFactory = (JackrabbitValueFactory) vf;

        // 1. test initiate
        BinaryUpload binaryUpload = valueFactory.initiateBinaryUpload(content.size(), 1);

        assertNotNull(binaryUpload);
        assertEquals(UPLOAD_TOKEN, binaryUpload.getUploadToken());

        // 2. simulate an "upload"
        blobContent = content;

        // 3. test complete
        Binary binary = valueFactory.completeBinaryUpload(binaryUpload.getUploadToken());

        assertNotNull(binary);
        assertEquals(content.size(), binary.getSize());

        // 4. test that we can use this binary in JCR
        BinaryAccessTestUtils.storeBinary(getAdminSession(), FILE_PATH, binary);

        binary = BinaryAccessTestUtils.getBinary(getAdminSession(), FILE_PATH);
        content.assertEqualsWith(binary.getStream());
    }
}