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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static com.google.common.base.StandardSystemProperty.USER_HOME;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension to {@link DataStoreUtils} to enable Azure extensions for cleaning and initialization.
 */
public class AzureDataStoreUtils extends DataStoreUtils {
    private static final Logger log = LoggerFactory.getLogger(AzureDataStoreUtils.class);

    private static final String DEFAULT_CONFIG_PATH = "./src/test/resources/azure.properties";
    private static final String DEFAULT_PROPERTY_FILE = "azure.properties";
    private static final String SYS_PROP_NAME = "azure.config";

    /**
     * Check for presence of mandatory properties.
     *
     * @return true if mandatory props configured.
     */
    public static boolean isAzureConfigured() {
        Properties props = getAzureConfig();
        //need either access keys or sas
        if (!props.containsKey(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY) && !props.containsKey(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME)
                && !(props.containsKey(AzureConstants.AZURE_BLOB_CONTAINER_NAME))) {
            if (!props.containsKey(AzureConstants.AZURE_SAS) && !props.containsKey(AzureConstants.AZURE_BLOB_ENDPOINT)
                    && !(props.containsKey(AzureConstants.AZURE_BLOB_CONTAINER_NAME))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Read any config property configured.
     * Also, read any props available as system properties.
     * System properties take precedence.
     *
     * @return Properties instance
     */
    public static Properties getAzureConfig() {
        String config = System.getProperty(SYS_PROP_NAME);
        if (Strings.isNullOrEmpty(config)) {
            File cfgFile = new File(USER_HOME.value(), DEFAULT_PROPERTY_FILE);
            if (cfgFile.exists()) {
                config = cfgFile.getAbsolutePath();
            }
        }
        if (Strings.isNullOrEmpty(config)) {
            config = DEFAULT_CONFIG_PATH;
        }

        Properties props = new Properties();
        if (new File(config).exists()) {
            InputStream is = null;
            try {
                is = new FileInputStream(config);
                props.load(is);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeQuietly(is);
            }
            props.putAll(getConfig());
            Map filtered = Maps.filterEntries(Maps.fromProperties(props), new Predicate<Map.Entry<? extends Object, ? extends Object>>() {
                @Override public boolean apply(Map.Entry<? extends Object, ? extends Object> input) {
                    return !Strings.isNullOrEmpty((String) input.getValue());
                }
            });
            props = new Properties();
            props.putAll(filtered);
        }
        return props;
    }

    static Iterable<?> getDataStorePropertyFixtures() {
        Properties propsAccessKey = AzureDataStoreUtils.getAzureConfig();
        Properties propsSAS = (Properties) propsAccessKey.clone();

        propsAccessKey.remove(AzureConstants.AZURE_SAS);
        propsSAS.remove(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);

        Collection<Properties> fixtures = Lists.newArrayList();
        if (propsAccessKey.containsKey(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY)) {
            fixtures.add(propsAccessKey);
        }
        if (propsSAS.containsKey(AzureConstants.AZURE_SAS)) {
            fixtures.add(propsSAS);
        }

        return fixtures;
    }

    public static DataStore getAzureDataStore(Properties props, String homeDir) throws Exception {
        AzureDataStore ds = new AzureDataStore();
        PropertiesUtil.populate(ds, Maps.fromProperties(props), false);
        ds.setProperties(props);
        ds.init(homeDir);

        return ds;
    }

    public static void deleteContainer(String containerName) throws Exception {
        if (Strings.isNullOrEmpty(containerName)) {
            log.warn("Cannot delete container with null or empty name. containerName={}", containerName);
            return;
        }
        log.info("Starting to delete container. containerName={}", containerName);
        Properties props = getAzureConfig();
        BlobContainerClient container = Utils.getBlobContainer(props, containerName);
        Response result = container.deleteWithResponse(null, null, Context.NONE);
        if (result.getStatusCode() < 400 || result.getStatusCode() == 404) {
            log.info("Container deleted. containerName={} existed={}", containerName, result.getStatusCode() != 404);
        }
        else {
            log.warn("Unable to delete container. containerName={}, status code={}",
                    containerName, result.getStatusCode());
        }
    }

    protected static HttpsURLConnection getHttpsConnection(long length, URI uri) throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) uri.toURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Content-Length", String.valueOf(length));
        conn.setRequestProperty("Date", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now()));
        conn.setRequestProperty("x-ms-version", "2017-11-09");

        return conn;
    }
}
