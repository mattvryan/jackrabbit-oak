package org.apache.jackrabbit.oak.blob.cloud.azureblob;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore.LOG;

public final class Utils {

    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    private static final String DELETE_CONFIG_SUFFIX = ";burn";

    public static final String DOT = ".";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private Utils() {

    }

    /**
     * Read a configuration properties file. If the file name ends with ";burn",
     * the file is deleted after reading.
     *
     * @param fileName the properties file name
     * @return the properties
     * @throws java.io.IOException if the file doesn't exist
     */
    public static Properties readConfig(String fileName) throws IOException {
        boolean delete = false;
        if (fileName.endsWith(DELETE_CONFIG_SUFFIX)) {
            delete = true;
            fileName = fileName.substring(0, fileName.length()
                    - DELETE_CONFIG_SUFFIX.length());
        }
        if (!new File(fileName).exists()) {
            throw new IOException("Config file not found: " + fileName);
        }
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(fileName);
            prop.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
            if (delete) {
                deleteIfPossible(new File(fileName));
            }
        }
        return prop;
    }

    private static void deleteIfPossible(final File file) {
        boolean deleted = file.delete();
        if (!deleted) {
            LOG.warn("Could not delete " + file.getAbsolutePath());
        }
    }
}
