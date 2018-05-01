package com.instaclustr.backup.tasks;

import com.google.cloud.AuthCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Injector;
import com.instaclustr.backup.service.TestHelperService;
import com.instaclustr.backup.downloader.Downloader;
import com.instaclustr.backup.downloader.GCPDownloader;
import com.instaclustr.backup.downloader.RemoteObjectReference;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GCPDownloaderTest {
    private TestHelperService testHelperService = new TestHelperService();

    Injector injector;
    Storage storage;

    Map<String, Path> tempDirs = new HashMap<String, Path>() {{
        this.put("test", null);
    }};

    private final org.slf4j.Logger log = LoggerFactory.getLogger(GCPDownloaderTest.class);

    // This test case depends upon a GCP cluster having been provisioned
    // Test Preprod cluster
    private final String nodeId = "29685ff7-53d5-4990-a6ac-898dc48e472f";
    private final String clusterDataCentreId = "a297edee-3a5e-4d70-a9c0-4d7f0e9a7a60";
    private final String clusterId = "6d74bf2e-6356-4ac1-828e-5a5333026f11";
    private final String backupBucket = "a297edee-3a5e-4d70-a9c0-4d7f0e9a7a60";

    private final String restoreFromNodeId = "29685ff7-53d5-4990-a6ac-898dc48e472f";
    private final String restoreFromCdcId = "a297edee-3a5e-4d70-a9c0-4d7f0e9a7a60";

    final String instaclustrKey = "XXXXXXXXXXXXXX";

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        testHelperService.setupTempDirectories(tempDirs);

        storage = StorageOptions.newBuilder()
            .setProjectId("instaclustr-dev")
            .setAuthCredentials(AuthCredentials.createForJson(new ByteArrayInputStream(instaclustrKey.getBytes()))).build().getService();


    }

    @Test(description = "Determine if file needs to be downloaded.")
    public void compareBeforeDownload() throws Exception {
        final long testTextSizeBytes = 29071;

//        GCPDownloader gcpDownloader = injector.getInstance(GCPDownloaderFactory.class).gcpDownloader(new ClusterDataCentrePrimaryKey(restoreFromCdcId), restoreFromNodeId);
        GCPDownloader gcpDownloader = new GCPDownloader(storage, restoreFromCdcId, restoreFromNodeId);
        RemoteObjectReference remoteObjectReference = gcpDownloader.objectKeyToRemoteReference(Paths.get("manifests/autosnap-1493352226"));
        final Downloader.CompareFilesResult compareFilesResult1 = gcpDownloader.compareRemoteObject(testTextSizeBytes, tempDirs.get("test"), remoteObjectReference);
        Assert.assertEquals(compareFilesResult1, Downloader.CompareFilesResult.DOWNLOAD_REQUIRED);

        // Expected Storage account structure is:
        // Buckets -> a297edee-3a5e-4d70-a9c0-4d7f0e9a7a60 (cdc id) -> 29685ff7-53d5-4990-a6ac-898dc48e472f (node id) -> manifests -> autosnap-1493352226
        gcpDownloader.downloadFile(tempDirs.get("test").resolve("snap"), remoteObjectReference);

        final Downloader.CompareFilesResult compareFilesResult2 = gcpDownloader.compareRemoteObject(testTextSizeBytes, tempDirs.get("test").resolve("snap"), remoteObjectReference);
        Assert.assertEquals(compareFilesResult2, Downloader.CompareFilesResult.MATCHING);
    }

    @Test(description = "Downloaded filepath should mirror GCP filepath.")
    public void downloadIntoFilepath() throws Exception {
        final String file1 = "data/instaclustr/recovery_codes-3bfaade02bc711e790bb5d79090533ce/manifest.json";
        final String file2 = "data/system/available_ranges-c539fcabd65a31d18133d25605643ee3/manifest.json";

        GCPDownloader gcpDownloader = new GCPDownloader(storage, restoreFromCdcId, restoreFromNodeId);
        RemoteObjectReference remoteObjectReference1 = gcpDownloader.objectKeyToRemoteReference(Paths.get(file1));
        gcpDownloader.downloadFile(tempDirs.get("test").resolve(file1), remoteObjectReference1);
        RemoteObjectReference remoteObjectReference2 = gcpDownloader.objectKeyToRemoteReference(Paths.get(file2));
        gcpDownloader.downloadFile(tempDirs.get("test").resolve(file1), remoteObjectReference2);

        Assert.assertTrue(tempDirs.get("test").resolve(file1).toFile().exists());
        Assert.assertTrue(tempDirs.get("test").resolve(file2).toFile().exists());
    }

    @Test(description = "Retrieve file list from GCP Storage")
    public void listFiles() throws Exception {
        GCPDownloader gcpDownloader = new GCPDownloader(storage, restoreFromCdcId, restoreFromNodeId);
        RemoteObjectReference filesReference = gcpDownloader.objectKeyToRemoteReference(Paths.get("commitlogs"));
        List<RemoteObjectReference> fileList = gcpDownloader.listFiles(filesReference);

        Assert.assertEquals(fileList.size(), 3);

        RemoteObjectReference file1Reference = fileList.get(0);
        gcpDownloader.downloadFile(tempDirs.get("test").resolve("file1"), file1Reference);

        final Path file1Path = tempDirs.get("test").resolve("file1");
        Assert.assertTrue(file1Path.toFile().exists());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp () throws IOException {
        testHelperService.deleteTempDirectories(tempDirs);
    }
}