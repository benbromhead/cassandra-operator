package com.instaclustr.backup.downloader;

import com.instaclustr.backup.RestoreArguments;
import com.instaclustr.backup.common.LocalFileObjectReference;
import com.instaclustr.backup.common.RemoteObjectReference;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LocalFileDownloader extends Downloader {
    private final Path sourceDirectory;

    public LocalFileDownloader(final RestoreArguments arguments) {
        super(arguments);
        this.sourceDirectory = Paths.get(arguments.backupBucket);
    }

    @Override
    public RemoteObjectReference objectKeyToRemoteReference(final Path objectKey) throws Exception {
        return new LocalFileObjectReference(objectKey);
    }

    @Override
    public void downloadFile(final Path localFilePath, final RemoteObjectReference object) throws Exception {
        Path remoteFilePath = Paths.get(((LocalFileObjectReference) object).canonicalPath);

        File localFileDirectory = localFilePath.getParent().toFile();
        if (!localFileDirectory.exists())
            localFileDirectory.mkdirs();

        Files.copy(remoteFilePath, localFilePath);
    }

    @Override
    public List<RemoteObjectReference> listFiles(final RemoteObjectReference prefix) throws Exception {
        final LocalFileObjectReference localFileObjectReference = (LocalFileObjectReference) prefix;

        final List<RemoteObjectReference> remoteObjectReferenceList = new ArrayList<>();

        List<Path> pathsList = Files.walk(Paths.get(localFileObjectReference.canonicalPath))
                .filter(filePath -> Files.isRegularFile(filePath))
                .collect(Collectors.toList());

        for (Path path : pathsList) {
            remoteObjectReferenceList.add(objectKeyToRemoteReference(sourceDirectory.resolve(restoreFromNodeId).relativize(path)));
        }

        return remoteObjectReferenceList;
    }

    @Override
    void cleanup() throws Exception {
        // Nothing to cleanup
    }
}
