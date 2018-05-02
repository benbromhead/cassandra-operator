package com.instaclustr.backup.tasks;

import com.instaclustr.backup.service.TestHelperService;
import com.instaclustr.backup.downloader.AzureDownloader;
import com.instaclustr.backup.downloader.Downloader;
import com.instaclustr.backup.downloader.RemoteObjectReference;
import com.instaclustr.backup.util.CloudDownloadUploadFactory;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureDownloaderTest {
    private TestHelperService testHelperService = new TestHelperService();


    Map<String, Path> tempDirs = new HashMap<String, Path>() {{
        this.put("test", null);
    }};

    private final org.slf4j.Logger log = LoggerFactory.getLogger(AzureDownloaderTest.class);

    // This test case depends upon an Azure cluster having been provisioned
    // Test Preprod cluster
    private final String nodeId = "e1c41688-7249-41b2-9d75-208ec6310f72";
    private final String clusterId = "f2056d84-8ca2-4162-8c69-e6036b6bbd4c";
    private final String azureStorageAccountName = "accName";
    private final String azureStorageAccountSasToken = "sig=YYYYYYYXXXXXXXXXXXX";
    private final String fakeSasToken = "Not going to work";
    private final String restoreFromNodeId = "e1c41688-7249-41b2-9d75-208ec6310f72";
    private final String restoreFromClusterId = "f2056d84-8ca2-4162-8c69-e6036b6bbd4c";

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {

    }

    @Test(description = "Determine if file needs to be downloaded.")
    public void compareBeforeDownload() throws Exception {
        final long testTextSizeBytes = 39482;
        AzureDownloader azureDownloader = new AzureDownloader(CloudDownloadUploadFactory.getCloudBlobClient() ,restoreFromClusterId, restoreFromNodeId);
        RemoteObjectReference remoteObjectReference = azureDownloader.objectKeyToRemoteReference(Paths.get("manifests/autosnap-1493343789"));
        final Downloader.CompareFilesResult compareFilesResult1 = azureDownloader.compareRemoteObject(testTextSizeBytes, tempDirs.get("test"), remoteObjectReference);
        Assert.assertEquals(compareFilesResult1, Downloader.CompareFilesResult.DOWNLOAD_REQUIRED);

        // Expected Storage account structure is:
        // icbackuptest -> Blob service -> 43223127-3fe5-42bb-abfe-5820adda0fdc (cluster id) -> 6bad712f-36c7-4f22-9cbe-f124f049e856 (node id) -> manifests -> autosnap-1493343789
        // Storage URL it should generate:
        // https://icbackuptest.blob.core.windows.net/43223127-3fe5-42bb-abfe-5820adda0fdc/43223127-3fe5-42bb-abfe-5820adda0fdc/6bad712f-36c7-4f22-9cbe-f124f049e856/manifests/autosnap-1493343789
        azureDownloader.downloadFile(tempDirs.get("test").resolve("snap"), remoteObjectReference);

        final Downloader.CompareFilesResult compareFilesResult2 = azureDownloader.compareRemoteObject(testTextSizeBytes, tempDirs.get("test").resolve("snap"), remoteObjectReference);
        Assert.assertEquals(compareFilesResult2, Downloader.CompareFilesResult.MATCHING);
    }

    @Test(description = "Downloaded filepath should mirror Azure filepath.")
    public void downloadIntoFilepath() throws Exception {
        final String file1 = "data/instaclustr/recovery_codes-e58a5a902ba411e7ab9759144dc7fe28/manifest.json";
        final String file2 = "data/system/available_ranges-c539fcabd65a31d18133d25605643ee3/manifest.json";

        AzureDownloader azureDownloader = new AzureDownloader(CloudDownloadUploadFactory.getCloudBlobClient() ,restoreFromClusterId, restoreFromNodeId);
        RemoteObjectReference remoteObjectReference1 = azureDownloader.objectKeyToRemoteReference(Paths.get(file1));
        azureDownloader.downloadFile(tempDirs.get("test").resolve(file1), remoteObjectReference1);
        RemoteObjectReference remoteObjectReference2 = azureDownloader.objectKeyToRemoteReference(Paths.get(file2));
        azureDownloader.downloadFile(tempDirs.get("test").resolve(file2), remoteObjectReference2);

        Assert.assertTrue(tempDirs.get("test").resolve(file1).toFile().exists());
        Assert.assertTrue(tempDirs.get("test").resolve(file2).toFile().exists());
    }

    @Test(description = "Retrieve file list from Azure Blob Storage")
    public void listFiles() throws Exception {
        AzureDownloader azureDownloader = new AzureDownloader(CloudDownloadUploadFactory.getCloudBlobClient() ,restoreFromClusterId, restoreFromNodeId);
        RemoteObjectReference filesReference = azureDownloader.objectKeyToRemoteReference(Paths.get("manifests"));
        List<RemoteObjectReference> fileList = azureDownloader.listFiles(filesReference);

        Assert.assertEquals(fileList.size(), 3);

        RemoteObjectReference file1Reference = fileList.get(0);
        azureDownloader.downloadFile(tempDirs.get("test").resolve("manifest"), file1Reference);

        final Path file1Path = tempDirs.get("test").resolve("manifest");
        Assert.assertTrue(file1Path.toFile().exists());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp () throws IOException {
        testHelperService.deleteTempDirectories(tempDirs);
    }
}