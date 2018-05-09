package com.instaclustr.backup.common;

import java.nio.file.Path;

public class LocalFileObjectReference extends RemoteObjectReference {

    public LocalFileObjectReference(final Path objectKey) {
        super(objectKey, null);
    }

    @Override
    public Path getObjectKey() {
        return null;
    }
}
