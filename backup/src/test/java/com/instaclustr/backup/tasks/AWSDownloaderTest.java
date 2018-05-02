package com.instaclustr.backup.tasks;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.instaclustr.backup.service.TestHelperService;
import com.instaclustr.backup.downloader.AWSDownloader;
import com.instaclustr.backup.downloader.Downloader;
import com.instaclustr.backup.downloader.RemoteObjectReference;
import com.instaclustr.backup.util.Directories;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AWSDownloaderTest {
    private TestHelperService testHelperService = new TestHelperService();

    Injector injector;

    Map<String, Path> tempDirs = new HashMap<String, Path>() {{
        this.put("test", null);
    }};

    private final org.slf4j.Logger log = LoggerFactory.getLogger(AWSDownloaderTest.class);

    private final String nodeId = "dummy-node-id";
    private final String clusterId = "dummy-cluster-id";
    private final String backupBucket = "test-awsdownloader";

    private final String restoreFromNodeId = "dummy-node-id";
    private final String restoreFromClusterId = "dummy-cluster-id";
    private final String restoreFromBackupBucket = "test-awsdownloader";
    private TransferManager transferManager;

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {

        testHelperService.setupTempDirectories(tempDirs);

        AmazonS3 s3Client = new AmazonS3Client(new ClientConfiguration()
                .withMaxConnections(10 * 10)
                .withSocketTimeout((int) TimeUnit.MINUTES.toMillis(5))
                .withConnectionTimeout((int) TimeUnit.MINUTES.toMillis(5))
        ).withRegion(Regions.US_EAST_1);

        transferManager = new TransferManager(s3Client);
    }

    @Test(description = "Determine if file needs to be downloaded.")
    public void compareBeforeDownload() throws Exception {
        final long testTextSize = 19;

//        AWSDownloader awsDownloader = new AWSDownloader(new ClusterPrimaryKey(restoreFromClusterId), restoreFromNodeId, restoreFromBackupBucket);
        AWSDownloader awsDownloader = new AWSDownloader(transferManager, restoreFromClusterId, restoreFromNodeId, restoreFromBackupBucket);
        // Pass in: node_id/manifests/autosnap-<snapshot_tag>
        // objectKeyToRemoteReference will then prepend the cluster ID
        RemoteObjectReference remoteObjectReference = awsDownloader.objectKeyToRemoteReference(Paths.get("test.txt"));
        final Downloader.CompareFilesResult compareFilesResult1 = awsDownloader.compareRemoteObject(testTextSize, tempDirs.get("test"), remoteObjectReference);
        Assert.assertEquals(compareFilesResult1, Downloader.CompareFilesResult.DOWNLOAD_REQUIRED);

        awsDownloader.downloadFile(tempDirs.get("test").resolve("snap"), remoteObjectReference);

        final Downloader.CompareFilesResult compareFilesResult2 = awsDownloader.compareRemoteObject(testTextSize, tempDirs.get("test").resolve("snap"), remoteObjectReference);
        Assert.assertEquals(compareFilesResult2, Downloader.CompareFilesResult.MATCHING);
    }

    @Test(description = "Downloaded filepath should mirror S3 filepath.")
    public void downloadIntoFilepath() throws Exception {
        AWSDownloader awsDownloader = new AWSDownloader(transferManager, restoreFromClusterId, restoreFromNodeId, restoreFromBackupBucket);
        RemoteObjectReference remoteObjectReference1 = awsDownloader.objectKeyToRemoteReference(Paths.get("data/dummy-table-1/dummy-table-1.txt"));
        awsDownloader.downloadFile(tempDirs.get("test").resolve("data/dummy-table-1/dummy-table-1.txt"), remoteObjectReference1);
        RemoteObjectReference remoteObjectReference2 = awsDownloader.objectKeyToRemoteReference(Paths.get("data/dummy-table-2/dummy-table-2.txt"));
        awsDownloader.downloadFile(tempDirs.get("test").resolve("data/dummy-table-2/dummy-table-2.txt"), remoteObjectReference2);

        Assert.assertTrue(tempDirs.get("test").resolve("data/dummy-table-1/dummy-table-1.txt").toFile().exists());
        Assert.assertTrue(tempDirs.get("test").resolve("data/dummy-table-2/dummy-table-2.txt").toFile().exists());
    }

    @Test(description = "Retrieve file list from S3")
    public void listFiles() throws Exception {
        AWSDownloader awsDownloader = new AWSDownloader(transferManager, restoreFromClusterId, restoreFromNodeId, restoreFromBackupBucket);
        RemoteObjectReference commitlogsReference = awsDownloader.objectKeyToRemoteReference(Paths.get(Directories.CASSANDRA_COMMIT_LOGS));
        List<RemoteObjectReference> fileList = awsDownloader.listFiles(commitlogsReference);

        Assert.assertEquals(fileList.size(), 3);

        RemoteObjectReference file1Reference = fileList.get(0);
        awsDownloader.downloadFile(tempDirs.get("test").resolve("test"), file1Reference);

        final Path file1Path = tempDirs.get("test").resolve("test");
        Assert.assertTrue(file1Path.toFile().exists());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp () throws IOException {
        testHelperService.deleteTempDirectories(tempDirs);
    }
}