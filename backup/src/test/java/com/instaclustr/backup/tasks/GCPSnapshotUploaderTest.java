package com.instaclustr.backup.tasks;

import com.google.cloud.AuthCredentials;
import com.google.cloud.storage.*;
import com.google.inject.*;

import com.instaclustr.backup.uploader.GCPSnapshotUploader;
import com.instaclustr.backup.uploader.RemoteObjectReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.testng.Assert.*;

@Test(groups = {"gcp", "all"})
public class GCPSnapshotUploaderTest {

    private Injector injector;
    private Path tempDir;
    private Path contentDir;
    private Path backupFile;
    private Path backupPath;
    private String nodeId;
    private String bucket;
    private Storage storage;
    private String cdcID;
    private String uploadURI;

    private final int size = 0; //UploadSnapshotFile accepts size as a parameter though for GCP it's not used.
    private final String backupContents = "TestGCPBackup - if you see this file lurking in a bucket somewhere, please delete it to save us much needed cents.";


    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        cdcID = "test-cdc-id";
        nodeId = "test-node";
        bucket = "unittestbucket";
        storage = StorageOptions.getDefaultInstance().getService();

        tempDir  = Files.createTempDirectory(cdcID);
        contentDir = Files.createDirectory(tempDir.resolve(cdcID));
        backupFile = contentDir.resolve("GCPBackuptestfile");

        try (final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(backupFile, StandardCharsets.UTF_8))) {
            writer.write("TestGCPBackup - if you see this file lurking in a bucket somewhere, please delete it to save us much needed cents.");
        }
    }


    @Test
    public void basicDirectoryBackupTest() throws Exception {
//        GCPSnapshotUploader gcpSnapshotUploader = injector.getInstance(GCPSnapshotUploader.class);
        GCPSnapshotUploader gcpSnapshotUploader = new GCPSnapshotUploader(storage, nodeId, cdcID, bucket);
        backupPath = tempDir.relativize(backupFile);
        uploadURI = Paths.get("test-node").resolve(backupPath).toString();

        try {
            storage.create(BucketInfo.of(bucket));
        } catch (StorageException e) {
            if (!e.getReason().equalsIgnoreCase("conflict"))
                throw e;
        }
        final RemoteObjectReference remoteObjectReference = gcpSnapshotUploader.objectKeyToRemoteReference(backupPath);
        gcpSnapshotUploader.uploadSnapshotFile(size, Files.newInputStream(backupFile), remoteObjectReference);
        Blob uploadedBlob = storage.get(bucket, uploadURI);

        assertNotNull(uploadedBlob); //Assert upload did succeed
        assertEquals(new String(uploadedBlob.getContent()), backupContents); //Assert the contents are what we expect

        //Do another upload of the same file. This should 'freshen' the blob.
        gcpSnapshotUploader.uploadSnapshotFile(size, Files.newInputStream(backupFile), remoteObjectReference);
        Blob freshenedBlob = storage.get(bucket, uploadURI);
        assertNotEquals(uploadedBlob.getCreateTime(), freshenedBlob.getCreateTime());

        //This won't actually clean anything, because nothing is older than 1 week, and you can't set 'createTime'
        //for the file you want to upload. Good to know the method runs at least!
        gcpSnapshotUploader.cleanup();
    }

    @AfterMethod
    public void cleanUp() {
        BlobId blob = BlobId.of(bucket, uploadURI);
        storage.delete(blob);
        Blob deletedBlob = storage.get(bucket, uploadURI);
        assertNull(deletedBlob);
        storage.delete(bucket);
    }
}
