package org.apache.jackrabbit.oak.blob.cloud.azure.abs;

public class AzureConstants {
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
