package org.apache.jackrabbit.oak.plugins.blob;

import org.apache.jackrabbit.core.data.DataIdentifier;

import java.io.File;

public class DataIdentifierCreationResult {
    private final DataIdentifier identifier;
    private final File tmpFile;

    public DataIdentifierCreationResult(final DataIdentifier identifier, final File tmpFile) {
        this.identifier = identifier;
        this.tmpFile = tmpFile;
    }

    public DataIdentifier getIdentifier() {
        return identifier;
    }

    public File getTmpFile() {
        return tmpFile;
    }
}
