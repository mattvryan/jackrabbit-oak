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

package org.apache.jackrabbit.oak.blob.cloud.azureblob;

public final class AzureConstants {
    public static final String AZURE_ACCOUNT_NAME = "accountName";
    public static final String AZURE_ACCOUNT_KEY = "accountKey";
    public static final String AZURE_REGION_NAME = "azureRegion";
    public static final String ABS_CONTAINER_NAME = "azureContainer";
    /**
     * Azure Blob Storage Http connection timeout.
     */
    public static final String ABS_CONN_TIMEOUT = "connectionTimeout";

    /**
     * Azure Blob Storage socket timeout.
     */
    public static final String ABS_SOCK_TIMEOUT = "socketTimeout";

    /**
     * Azure Blob Storage maximum connections to be used.
     */
    public static final String ABS_MAX_CONNS = "maxConnections";

    /**
     * Azure Blob Storage maximum retries.
     */
    public static final String ABS_MAX_ERR_RETRY = "maxErrorRetry";

    /**
     * Constant to rename keys
     */
    public static final String ABS_WRITE_THREADS = "writeThreads";

    /**
     *  Constant to set proxy host.
     */
    public static final String PROXY_HOST = "proxyHost";

    /**
     *  Constant to set proxy port.
     */
    public static final String PROXY_PORT = "proxyPort";

    private AzureConstants() { }
}
