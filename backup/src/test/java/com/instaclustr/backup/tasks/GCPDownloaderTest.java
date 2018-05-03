package com.instaclustr.backup.tasks;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.instaclustr.backup.downloader.Downloader;
import com.instaclustr.backup.downloader.GCPDownloader;
import com.instaclustr.backup.downloader.RemoteObjectReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

@Test(groups = {"gcp", "all"})
public class GCPDownloaderTest extends AbstractBackupTest {

    private Storage storage;
    // TODO: Change test to not depend on Instaclustr service

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        super.setup();
        storage = StorageOptions.getDefaultInstance().getService();
    }

    @Test(description = "Determine if file needs to be downloaded.")
    public void compareBeforeDownload() throws Exception {
        final long testTextSizeBytes = 29071;

//        GCPDownloader gcpDownloader = injector.getInstance(GCPDownloaderFactory.class).gcpDownloader(new ClusterDataCentrePrimaryKey(sourceBucket), restoreFromNodeId);
        GCPDownloader gcpDownloader = new GCPDownloader(storage, sourceBucket, restoreFromNodeId);
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

        GCPDownloader gcpDownloader = new GCPDownloader(storage, sourceBucket, restoreFromNodeId);
        RemoteObjectReference remoteObjectReference1 = gcpDownloader.objectKeyToRemoteReference(Paths.get(file1));
        gcpDownloader.downloadFile(tempDirs.get("test").resolve(file1), remoteObjectReference1);
        RemoteObjectReference remoteObjectReference2 = gcpDownloader.objectKeyToRemoteReference(Paths.get(file2));
        gcpDownloader.downloadFile(tempDirs.get("test").resolve(file1), remoteObjectReference2);

        Assert.assertTrue(tempDirs.get("test").resolve(file1).toFile().exists());
        Assert.assertTrue(tempDirs.get("test").resolve(file2).toFile().exists());
    }

    @Test(description = "Retrieve file list from GCP Storage")
    public void listFiles() throws Exception {
        GCPDownloader gcpDownloader = new GCPDownloader(storage, sourceBucket, restoreFromNodeId);
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