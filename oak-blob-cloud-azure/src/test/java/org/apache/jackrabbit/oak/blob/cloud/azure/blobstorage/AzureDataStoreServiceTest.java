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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStoreUtils.getAzureConfig;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStoreUtils.isAzureConfigured;
import static org.junit.Assume.assumeTrue;

import java.util.Properties;

import org.apache.jackrabbit.oak.blob.cloud.AbstractCloudDataStoreService;
import org.apache.jackrabbit.oak.blob.cloud.AbstractCloudDataStoreServiceTest;
import org.junit.BeforeClass;

public class AzureDataStoreServiceTest extends AbstractCloudDataStoreServiceTest {
    @Override
    protected boolean isServiceConfigured() {
        return isAzureConfigured();
    }

    @BeforeClass
    public static void checkAssumptions() {
        assumeTrue(isAzureConfigured());
    }

    @Override
    protected Properties getServiceConfig() {
        return getAzureConfig();
    }

    @Override
    protected AbstractCloudDataStoreService getServiceInstance() {
        return new AzureDataStoreService();
    }
}
