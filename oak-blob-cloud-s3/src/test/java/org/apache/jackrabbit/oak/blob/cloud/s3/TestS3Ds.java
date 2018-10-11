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
package org.apache.jackrabbit.oak.blob.cloud.s3;

import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.deleteBucket;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getFixtures;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3Config;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.getS3DataStore;
import static org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils.isS3Configured;
import static org.junit.Assume.assumeTrue;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.jcr.RepositoryException;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.blob.cloud.AbstractCloudDataStoreTest;
import org.apache.jackrabbit.oak.blob.cloud.CloudDataStore;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link S3DataStore} with S3Backend and local cache on.
 * It requires to pass aws config file via system property or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties
 */
@RunWith(Parameterized.class)
public class TestS3Ds extends AbstractCloudDataStoreTest {

    protected static final Logger LOG = LoggerFactory.getLogger(TestS3Ds.class);

    private static final Set<String> createdBuckets = Sets.newHashSet();
    private static Date startTime = null;

    protected String bucket = null;

    @Parameterized.Parameter
    public String s3Class;

    @Parameterized.Parameters(name = "{index}: ({0})")
    public static List<String> fixtures() {
        return getFixtures();
    }

    @BeforeClass
    public static void assumptions() {
        startTime = DateUtils.addSeconds(new Date(), -60);
        assumeTrue(isS3Configured());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        bucket = container;
        createdBuckets.add(bucket);
    }

    @AfterClass
    public static void cleanupBuckets() {
        for (final String bucket : createdBuckets) {
            try {
                deleteBucket(bucket, startTime);
            }
            catch (Exception ignore) { }
        }
    }

    @Override
    protected DataStore createDataStore() throws RepositoryException {
        CloudDataStore s3ds = null;
        try {
            s3ds = getS3DataStore(s3Class, props, dataStoreDir);
            //s3ds = getDataStoreInstance(props, dataStoreDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sleep(1000);
        return s3ds;
    }

    /**----------Not supported-----------**/
    @Override
    public void testUpdateLastModifiedOnAccess() {
    }

    @Override
    public void testDeleteAllOlderThan() {
    }


    //--- AbstractCloudDataStoreTest ---

    @Override
    protected boolean isDataStoreConfigured() {
        return isS3Configured();
    }

    @NotNull
    @Override
    protected Properties getDataStoreConfig(@NotNull final String containerName) {
        Properties props = getS3Config();
        props.setProperty(S3Constants.S3_BUCKET, containerName);
        return props;
    }

    @Override
    protected void deleteContainer(@NotNull final String containerName) throws Exception {
        deleteBucket(containerName, startTime);
    }
}
