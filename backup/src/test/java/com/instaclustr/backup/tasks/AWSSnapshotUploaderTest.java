package com.instaclustr.backup.tasks;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Optional;
import com.instaclustr.backup.uploader.AWSSnapshotUploader;
import com.instaclustr.backup.uploader.RemoteObjectReference;
import com.amazonaws.services.s3.transfer.*;
import com.instaclustr.backup.uploader.SnapshotUploader;
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
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

public class AWSSnapshotUploaderTest {
    private final String accessKey = "XXXXXXXXXX";
    private final String secretKey = "YYYYYYYYYYYYYYY";

    private final String bucket  = "test-awsuploader";
    private final String nodeID = "test-node";
    private final String cdcID = "test-cdc-id";
    private Path backupFile;
    private Path backupPath;
    private String canonicalPath;
    private AmazonS3 s3Client;
    private TransferManager storage;

    private final String backupContents = "TestFileUpload - if you see this file lurking in a bucket somewhere, please delete it to save us much needed cents.";
    private final int size = backupContents.length();

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {

        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        s3Client = new AmazonS3Client(credentialsProvider, new ClientConfiguration()
                .withMaxConnections(10 * 10)
                .withSocketTimeout((int) TimeUnit.MINUTES.toMillis(5))
                .withConnectionTimeout((int) TimeUnit.MINUTES.toMillis(5))
        ).withRegion(Regions.US_EAST_1);

        storage = new TransferManager(s3Client);

        s3Client.createBucket(bucket);
        System.out.println("Ensured bucket exists: " + bucket);

        Path tempDir  = Files.createTempDirectory(cdcID);
        System.out.println("Created Temp Directory:" + tempDir);

        final Path contentDir = Files.createDirectory(tempDir.resolve(cdcID));
        backupFile = contentDir.resolve("backuptestfile");
        backupPath = tempDir.relativize(backupFile);
        canonicalPath = Paths.get(cdcID).resolve(nodeID).resolve(backupPath).toString();

        try (final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(backupFile, StandardCharsets.UTF_8))) {
            writer.write(backupContents);
        }
        System.out.println("Created backup file" + backupFile);
    }

    @Test
    public void basicUploadTest() throws Exception {
        AWSSnapshotUploader uploader = new AWSSnapshotUploader(storage, nodeID, cdcID, bucket, Optional.absent());

        final RemoteObjectReference remoteObjectReference = uploader.objectKeyToRemoteReference(backupPath);
        uploader.uploadSnapshotFile(size, Files.newInputStream(backupFile), remoteObjectReference);
        uploader.close();

        final S3Object s3Object = s3Client.getObject(bucket, canonicalPath);
        final Date uploadTs = s3Object.getObjectMetadata().getLastModified();
        byte[] data = new byte[1024];
        assertEquals(s3Object.getObjectContent().read(data), size);
        assertEquals(new String(Arrays.copyOf(data, size)), backupContents); //Assert the contents are what we expect

        //Do another upload of the same file. This should 'freshen' the blob.
        assertEquals(uploader.freshenRemoteObject(remoteObjectReference), SnapshotUploader.FreshenResult.FRESHENED);
        final Date freshenTS = s3Client.getObject(bucket, canonicalPath).getObjectMetadata().getLastModified();
        assertNotEquals(uploadTs, freshenTS);
    }

    @Test
    public void encryptedUploadTest() throws Exception {
//        final String kmsId = "bab0acec-e44f-43eb-9197-d4874fe69b46";
//        AWSSnapshotUploader uploader = new AWSSnapshotUploader(storage, nodeID, cdcID, bucket, Optional.of(kmsId));
//
//        final RemoteObjectReference remoteObjectReference = uploader.objectKeyToRemoteReference(backupPath);
//        uploader.uploadSnapshotFile(size, Files.newInputStream(backupFile), remoteObjectReference);
//        uploader.close();
//
//        final S3Object s3Object = s3Client.getObject(bucket, canonicalPath);
//
//        final ObjectMetadata objectMetadata = s3Object.getObjectMetadata();
//        final Date uploadTs = objectMetadata.getLastModified();
//        byte[] data = new byte[1024];
//        assertEquals(s3Object.getObjectContent().read(data), size);
//        assertEquals(new String(Arrays.copyOf(data, size)), backupContents); //Assert the contents are what we expect
//
//        String KmsId = objectMetadata.getSSEAwsKmsKeyId();
//        assertNotNull(KmsId);
//        assertTrue(objectMetadata.getSSEAwsKmsKeyId().endsWith(kmsId));
//
//        //Do another upload of the same file. This should 'freshen' the blob.
//        assertEquals(uploader.freshenRemoteObject(remoteObjectReference), SnapshotUploader.FreshenResult.FRESHENED);
//        final ObjectMetadata objectMetadataFreshen = s3Client.getObject(bucket, canonicalPath).getObjectMetadata();
//        final Date freshenTS = objectMetadataFreshen.getLastModified();
//        assertNotEquals(uploadTs, freshenTS);
//        String KmsIdFreshen = objectMetadataFreshen.getSSEAwsKmsKeyId();
//        assertNotNull(KmsIdFreshen);
//        assertTrue(objectMetadataFreshen.getSSEAwsKmsKeyId().endsWith(KmsIdFreshen));
    }


    @AfterMethod
    public void cleanUp() {
        s3Client.deleteObject(bucket, canonicalPath);
    }
}
