package org.apache.jackrabbit.oak.plugins.blob;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.util.TransientFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

public class DataIdentifierFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DataIdentifierFactory.class);
    private static String DIGEST = System.getProperty("ds.digest.algorithm", "SHA-256");
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    public static DataIdentifierCreationResult createIdentifier(final InputStream inputStream, final File tmpDir) throws NoSuchAlgorithmException, IOException {
        Stopwatch watch = Stopwatch.createStarted();
        TransientFileFactory fileFactory = TransientFileFactory.getInstance();
        File tmpFile = fileFactory.createTransientFile("upload", null, tmpDir);

        // Copy the stream to the temporary file and calculate the
        // stream length and the message digest of the stream
        MessageDigest digest = MessageDigest.getInstance(DIGEST);
        OutputStream output = new DigestOutputStream(new FileOutputStream(tmpFile), digest);
        long length = 0;
        try {
            length = IOUtils.copyLarge(inputStream, output);
        } finally {
            output.close();
        }

        DataIdentifier identifier = new DataIdentifier(encodeHexString(digest.digest()));
        LOG.debug("SHA-256 of [{}], length =[{}] took [{}] ms ", identifier, length,
                watch.elapsed(TimeUnit.MILLISECONDS));

        return new DataIdentifierCreationResult(identifier, tmpFile);
    }

    private static String encodeHexString(byte[] value) {
        char[] buffer = new char[value.length * 2];
        for (int i = 0; i < value.length; i++) {
            buffer[2 * i] = HEX[(value[i] >> 4) & 0x0f];
            buffer[2 * i + 1] = HEX[value[i] & 0x0f];
        }
        return new String(buffer);
    }
}
