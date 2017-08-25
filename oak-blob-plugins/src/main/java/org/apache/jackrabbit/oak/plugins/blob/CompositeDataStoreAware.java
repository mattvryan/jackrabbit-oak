package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;

import java.io.File;

public interface CompositeDataStoreAware {
    DataRecord addRecord(DataIdentifier identifier, File tmpFile);
    DataRecord addRecord(DataIdentifier identifier, File tmpFile, BlobOptions options);
}
