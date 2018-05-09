package com.instaclustr.backup.tasks;

import com.google.inject.*;
import com.instaclustr.backup.CommitLogBackupArguments;
import com.instaclustr.backup.task.CommitLogBackupTask;
import com.instaclustr.backup.util.CloudDownloadUploadFactory;
import com.instaclustr.backup.uploader.FilesUploader;
import com.instaclustr.backup.util.Directories;
import com.instaclustr.backup.util.GlobalLock;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertTrue;

public class CommitLogBackupTaskTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CommitLogBackupTaskTest.class);
    private Injector injector;
    private Path rootDir;
    private final Path defaultCommitLogArchive = Paths.get("commitlog_archive");
    private final Path commitLogArchiveOverride = Paths.get("commitlog_override");
    private List<String> expectedCommitLogs;
    private CommitLogBackupArguments commitLogBackupArguments;
    private GlobalLock mockGlobalLock;

    @BeforeMethod(alwaysRun = true)
    public void setup() throws IOException, URISyntaxException {
        rootDir = Files.createTempDirectory(this.getClass().getSimpleName());
        commitLogBackupArguments = new CommitLogBackupArguments(this.getClass().getSimpleName(), null);
        mockGlobalLock = new GlobalLock(rootDir.toAbsolutePath().toString());
        mockGlobalLock.getLock(true);

    }

    @AfterMethod(alwaysRun = true)
    public void CleanUp() throws IOException {
        logger.info("About to Delete Temp Directory: {}", rootDir);

        FileUtils.deleteDirectory(rootDir.toFile());

        logger.info("Deleted Temp Directory: {}", rootDir);
        expectedCommitLogs.clear();
    }

    private void populateCommitLogArchive(final Path relativeCommitLogArchive) throws IOException, URISyntaxException {
        expectedCommitLogs = new ArrayList<>();
        final Path commitLogArchive = rootDir.resolve(relativeCommitLogArchive);
        Files.createDirectory(commitLogArchive);

        try (final DirectoryStream<Path> commitLogs = Files.newDirectoryStream(
                Paths.get(this.getClass().getClassLoader().getResource("dummybackup/DUMMY_NODE_ID/commitlog").toURI()))) {
            for (final Path commitLog : commitLogs) {
                Files.copy(commitLog, commitLogArchive.resolve(commitLog.getFileName()));
                expectedCommitLogs.add(commitLog.getFileName().toString());
            }
        }
    }

    private void runTest() throws Exception {
        logger.info("Assert we have commit logs to upload. Count: {}", expectedCommitLogs.size());
        assertTrue(expectedCommitLogs.size() > 0);

        //TODO: commitLogBackupArguments), commitLogBackupArguments), commitLogBackupArguments
        new CommitLogBackupTask(rootDir, defaultCommitLogArchive, mockGlobalLock, new FilesUploader(commitLogBackupArguments), commitLogBackupArguments).call();

        logger.info("Assert backup directory exists");
        final File backupDirectory = rootDir.resolve(Directories.CASSANDRA_COMMIT_LOGS).toFile();
        assertTrue(backupDirectory.exists());
        assertTrue(backupDirectory.isDirectory());

        logger.info("Assert number of files matches");
        Assert.assertEquals(backupDirectory.list().length, expectedCommitLogs.size());
    }

    @Test
    public void commitLogBackupTaskTest() throws Exception {
        logger.info("Testing CommitLogBackupTask with default commitlog_archive path");
        populateCommitLogArchive(defaultCommitLogArchive);

        runTest();

        logger.info("Assert commit log override path does not exists");
        Assert.assertFalse(Files.exists(rootDir.resolve(commitLogArchiveOverride)));
    }

    @Test
    public void commitLogBackupOverrideTest() throws Exception {
        logger.info("Testing CommitLogBackupTask with overriden commitlog_archive path");
        commitLogBackupArguments.commitLogArchiveOverride = commitLogArchiveOverride;
        populateCommitLogArchive(commitLogBackupArguments.commitLogArchiveOverride);

        runTest();

        logger.info("Assert default commit log path does not exists");
        Assert.assertFalse(Files.exists(rootDir.resolve(defaultCommitLogArchive)));
    }
}