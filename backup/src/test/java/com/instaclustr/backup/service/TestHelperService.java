package com.instaclustr.backup.service;

import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class TestHelperService {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TestHelperService.class);

    public void setupTempDirectories(Map<String, Path> tempDirectories) throws IOException, URISyntaxException {
        tempDirectories.forEach((name, path) -> {
            try {
                Iterator<String> folderIterator = Arrays.asList(name.split("/")).iterator();

                if (!folderIterator.hasNext()) {
                    return;
                }

                final String firstFolder = folderIterator.next();

                if ((!tempDirectories.containsKey(firstFolder)) || (tempDirectories.get(firstFolder) == null)) {
                    tempDirectories.put(firstFolder, Files.createTempDirectory(firstFolder));
                    log.debug("Creating temp directory:" + tempDirectories.get(firstFolder));
                }

                String subFolders = firstFolder;
                Path subFoldersPath = tempDirectories.get(firstFolder);

                while (folderIterator.hasNext()) {
                    String subFolder = folderIterator.next();
                    subFolders += "/" + subFolder;
                    subFoldersPath = subFoldersPath.resolve(subFolder);

                    if (!tempDirectories.containsKey(subFolders) || (tempDirectories.get(subFolders) == null)) {
                        Files.createDirectory(subFoldersPath);
                        tempDirectories.put(subFolders, subFoldersPath);
                        log.debug("Creating temp directory:" + tempDirectories.get(subFolders));
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public void deleteTempDirectories(Map<String, Path> tempDirectories) {
        tempDirectories.forEach((name, path) -> {
            try {
                File f = path.toFile();
                FileUtils.deleteDirectory(f);
                log.info("Deleted temp directory: " + path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
