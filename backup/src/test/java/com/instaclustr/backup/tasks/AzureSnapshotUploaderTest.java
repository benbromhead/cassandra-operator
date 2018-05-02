package com.instaclustr.backup.tasks;

import com.instaclustr.backup.util.CloudDownloadUploadFactory;
import com.instaclustr.backup.uploader.AzureSnapshotUploader;
import com.instaclustr.backup.uploader.RemoteObjectReference;
import com.microsoft.azure.storage.blob.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.testng.Assert.*;

public class AzureSnapshotUploaderTest {

    private final String nodeID = "2171eab7-0aa9-4ae6-9445-bbe30c09cc9c";
    private final String cdcID = "ab227317-2a53-42fb-8617-31bd6e53fa74";
    private final String clusterID = "f2056d84-8ca2-4162-8c69-e6036b6bbd4c";
    private final String container = "FOOOOO";
    private Path backupFile;
    private Path backupPath;
    private String canonicalPath;
    private CloudBlobClient storage;

    private final String backupContents = "TestFileUpload - if you see this file lurking in a bucket somewhere, please delete it to save us much needed cents.";
    private final int size = backupContents.length();

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        storage = CloudDownloadUploadFactory.getCloudBlobClient();

        Path tempDir  = Files.createTempDirectory(clusterID);
        System.out.println("Created Temp Directory:" + tempDir);

        final Path contentDir = Files.createDirectory(tempDir.resolve(clusterID));
        backupFile = contentDir.resolve("backuptestfile");
        backupPath = tempDir.relativize(backupFile);
        canonicalPath = Paths.get(clusterID).resolve(nodeID).resolve(backupPath).toString();

        try (final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(backupFile, StandardCharsets.UTF_8))) {
            writer.write(backupContents);
        }
        System.out.println("Created backup file" + backupFile);
    }

    @Test
    public void basicUploadTest() throws Exception {
        AzureSnapshotUploader uploader = new AzureSnapshotUploader(nodeID, clusterID, container, storage);

        final RemoteObjectReference remoteObjectReference = uploader.objectKeyToRemoteReference(backupPath);
        uploader.uploadSnapshotFile(size, Files.newInputStream(backupFile), remoteObjectReference);
        uploader.close();

        final CloudBlobContainer blobContainer = storage.getContainerReference(clusterID);

        Iterable<ListBlobItem> blobItemsIterable = blobContainer.listBlobs("", true, EnumSet.noneOf(BlobListingDetails.class), null, null);
        Iterator<ListBlobItem> blobItemsIterator = blobItemsIterable.iterator();

        List<String> fileList = new ArrayList<>();

        try {
            while (blobItemsIterator.hasNext()) {
                ListBlobItem listBlobItem = blobItemsIterator.next();
                fileList.add(listBlobItem.getUri().getPath());
            }
        } catch (NoSuchElementException e) {
            // Expect a "NoSuchElementException" exception when trying to list files.
        }

        assertTrue(fileList.size() > 0);

        final CloudBlockBlob blob = blobContainer.getBlockBlobReference(canonicalPath);

        final Date uploadTs = blob.getProperties().getLastModified();
        assertEquals(blob.downloadText(), backupContents); //Assert the contents are what we expect

        //Do another upload of the same file. This should 'freshen' the blob.
        uploader.uploadSnapshotFile(size, Files.newInputStream(backupFile), remoteObjectReference);
        final Date freshenTS = blob.getProperties().getLastModified();
        assertNotEquals(uploadTs, freshenTS);
    }

    @AfterMethod
    public void cleanUp() throws Exception {
        final CloudBlobContainer blobContainer = storage.getContainerReference(clusterID);
        final CloudBlockBlob blob = blobContainer.getBlockBlobReference(canonicalPath);
        blob.delete();
    }
}
