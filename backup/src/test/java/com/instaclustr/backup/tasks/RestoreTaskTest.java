package com.instaclustr.backup.tasks;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.instaclustr.backup.RestoreArguments;
import com.instaclustr.backup.downloader.LocalFileDownloader;
import com.instaclustr.backup.service.TestHelperService;
import com.instaclustr.backup.task.RestorePredicates;
import com.instaclustr.backup.task.RestoreTask;
import com.instaclustr.backup.util.Directories;
import com.instaclustr.backup.util.GlobalLock;
import org.apache.commons.io.FileUtils;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RestoreTaskTest {
    private Map<String, Path> tempDirs = new LinkedHashMap<String, Path>() {{
        this.put("sharedContainerRoot", null);
        this.put("sharedContainerRoot/data", null);
        this.put("sharedContainerRoot/hints", null);
        this.put("dummyRemoteSource", null);
        this.put("dummyCassandraYaml", null);
        this.put("etc", null);
        this.put("etc/cassandra", null);
    }};

    private final String tag = "dummy-snapshot-tag"; // Snapshot tag to restore

    private File manifestFile;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RestoreTaskTest.class);

    private static final String restoreToNodeId = UUID.randomUUID().toString();
    private static final String restoreToClusterId = UUID.randomUUID().toString();
    private static final String restoreToBackupBucket = "instaclustr-us-east-1-backups-test";

    private static final String restoreFromNodeId = "DUMMY_NODE_ID";
    private static final String restoreFromClusterId = "DUMMY_CLUSTER_ID";
    private static final String restoreFromBackupBucket = "DUMMY_BACKUP_BUCKET";

    // Intentionally left slash off start
    private static final String commitlogRestoreDirectory = "var/lib/cassandra/commitlog_restore";

    private final TestHelperService testHelperService = new TestHelperService();

    private Path payloadLocked;

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        testHelperService.setupTempDirectories(tempDirs);
        createDummySources();

        payloadLocked = tempDirs.get("sharedContainerRoot").resolve(".payload-locked");

        manifestFile = new File(tempDirs.get("sharedContainerRoot").resolve("manifests/" + tag).toString());

    }

    @BeforeMethod(alwaysRun=true)
    public void cleanDirectories() throws IOException, URISyntaxException {
        Iterator<Map.Entry<String, Path>> tempDirsIterator = tempDirs.entrySet().iterator();

        while(tempDirsIterator.hasNext()) {
            Map.Entry<String, Path> entry = tempDirsIterator.next();

            if (entry.getKey().equals("dummyRemoteSource")) {
                continue;
            }

            FileUtils.deleteDirectory(entry.getValue().toFile());
            Files.createDirectories(entry.getValue());
        }

        // Re-copy clean yaml
        final Path dummyConfPath = Paths.get(this.getClass().getClassLoader().getResource("cassandra/cassandra.yaml").toURI());
        Path dest = tempDirs.get("etc/cassandra").resolve(dummyConfPath.getFileName());
        FileUtils.copyURLToFile(dummyConfPath.toUri().toURL(), dest.toFile());
    }

    private void createDummySources() throws IOException, URISyntaxException {
        Path dummyRestorePath = Paths.get(this.getClass().getClassLoader().getResource("dummyrestore").toURI());

        Files.walkFileTree(dummyRestorePath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path dummyFileFullPath, final BasicFileAttributes attrs) throws IOException {
                final Path dummyFileRelativePath = dummyRestorePath.relativize(dummyFileFullPath);
                final Path destination = tempDirs.get("dummyRemoteSource").resolve(dummyFileRelativePath);
                FileUtils.copyURLToFile(dummyFileFullPath.toUri().toURL(), destination.toFile());

                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test(description = "Full restore to an existing cluster")
    public void basicRestore() throws Exception {
        final RestoreArguments restoreArguments = new RestoreArguments("cassandra-restore", null);
        restoreArguments.sourceBackupID = restoreFromNodeId;
        restoreArguments.clusterId = restoreFromClusterId;
        restoreArguments.backupBucket = restoreFromBackupBucket;
        restoreArguments.snapshotTag = tag;

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));

        calendar.set(2017, Calendar.MAY, 2, 2, 6, 0);
        restoreArguments.timestampStart = calendar.getTimeInMillis();
        calendar.set(2017, Calendar.MAY, 2, 2, 7, 0);
        restoreArguments.timestampEnd = calendar.getTimeInMillis();

        Multimap<String, String> keyspaceTableSubset = HashMultimap.create();

        final Path oldHintsFile = tempDirs.get("sharedContainerRoot/hints").resolve("test-hints-file");
        final Path oldCommitLog = tempDirs.get("sharedContainerRoot/hints").resolve("test-commitlog-file");
        final Path oldSavedCache = tempDirs.get("sharedContainerRoot/hints").resolve("test-save-cache-file");
        final Path oldDataFile = tempDirs.get("sharedContainerRoot").resolve("data/test/test-57c498c01ff511e792786bd71a520b1a/mc-7-big-Filter.db");
        final Path oldCRCFile = tempDirs.get("sharedContainerRoot").resolve("data/test/test-57c498c01ff511e792786bd71a520b1a/mc-7-big-Digest.crc32");
        final Path oldSystemDir = tempDirs.get("sharedContainerRoot").resolve("data/system");

        Files.createFile(oldHintsFile);
        Files.createFile(oldCommitLog);
        Files.createFile(oldSavedCache);
        Files.createDirectory(oldSystemDir);

        final String oldDataFileText = UUID.randomUUID().toString();
        oldDataFile.getParent().toFile().mkdirs();
        Files.write(oldDataFile, oldDataFileText.getBytes(), StandardOpenOption.CREATE_NEW);
        Files.write(oldCRCFile, "100".getBytes(), StandardOpenOption.CREATE_NEW);

        new RestoreTask(new LocalFileDownloader(tempDirs.get("dummyRemoteSource"), restoreFromNodeId),
                tempDirs.get("sharedContainerRoot"),
                tempDirs.get("etc/cassandra"),
                tempDirs.get("sharedContainerRoot"),
                new GlobalLock(tempDirs.get("sharedContainerRoot").toString()),
                restoreArguments,
                HashMultimap.create()).call();

        // Confirm that old files are cleared out
        Assert.assertFalse(oldHintsFile.toFile().exists());
        Assert.assertFalse(oldCommitLog.toFile().exists());
        Assert.assertFalse(oldSavedCache.toFile().exists());

        // Confirm manifest downloaded
        Assert.assertTrue(manifestFile.exists());

        // Confirm SSTables present
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(Directories.CASSANDRA_DATA).resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-7-big-Filter.db").toFile().exists());
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(Directories.CASSANDRA_DATA).resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-7-big-Statistics.db").toFile().exists());
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(Directories.CASSANDRA_DATA).resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-2-big-Filter.db").toFile().exists());
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(Directories.CASSANDRA_DATA).resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-2-big-Statistics.db").toFile().exists());

        // Confirm old SSTable was left in place
        Assert.assertEquals(String.join(",", Files.readAllLines(oldDataFile)), oldDataFileText);

        // Confirm relevant commitlog files downloaded
        // 1493690640000 = 2017-05-02 02:04:00
        Assert.assertFalse(tempDirs.get("sharedContainerRoot").resolve(commitlogRestoreDirectory).resolve("CommitLog-6-1496098988000.log").toFile().exists());
        // 1493690700000 = 2017-05-02 02:05:00
        Assert.assertFalse(tempDirs.get("sharedContainerRoot").resolve(commitlogRestoreDirectory).resolve("CommitLog-6-1496098988001.log").toFile().exists());
        // 1493690810201 = 2017-05-02 02:06:50.201
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(commitlogRestoreDirectory).resolve("CommitLog-6-1496098988002.log").toFile().exists());
        // 1493690810202 = 2017-05-02 02:06:50.202
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(commitlogRestoreDirectory).resolve("CommitLog-6-1496098988003.log").toFile().exists());
        // 1493690880000 = 2017-05-02 02:08:00
        Assert.assertTrue(tempDirs.get("sharedContainerRoot").resolve(commitlogRestoreDirectory).resolve("CommitLog-6-1496098988004.log").toFile().exists());
        // 1493690890000 = 2017-05-02 02:08:10
        Assert.assertFalse(tempDirs.get("sharedContainerRoot").resolve(commitlogRestoreDirectory).resolve("CommitLog-6-1496098988005.log").toFile().exists());

        // Confirm commitlog_archiving.properties present and set to expected values
        final Path commitlogArchivingProperties = tempDirs.get("etc/cassandra").resolve("commitlog_archiving.properties");
        Assert.assertTrue(commitlogArchivingProperties.toFile().exists());
        List<String> commitlogArchivingPropertiesText = Files.readAllLines(commitlogArchivingProperties);
        Assert.assertTrue(commitlogArchivingPropertiesText.get(2).contains("restore_point_in_time="));

        // Confirm cassandra.yaml present and includes tokens
        final Path cassandraYaml = tempDirs.get("etc/cassandra").resolve("cassandra.yaml");
        Assert.assertTrue(cassandraYaml.toFile().exists());
        String cassandraYamlText = new String(Files.readAllBytes(cassandraYaml));
        Assert.assertTrue(cassandraYamlText.contains("initial_token: "));
        Assert.assertTrue(cassandraYamlText.contains("auto_bootstrap: false"));
        Assert.assertFalse(cassandraYamlText.contains("auto_bootstrap: true"));
        Assert.assertTrue(cassandraYamlText.contains("cluster_name: alwyn-restore"));
        Assert.assertTrue(cassandraYamlText.contains("server_encryption_options: {keystore: /etc/cassandra/keystore.jks, truststore: /etc/cassandra/truststore.jks"));
    }

    @Test(description = "Table subset restore to an existing cluster")
    public void tableSubsetRestore() throws Exception {
        final Path cassandraDataDir = tempDirs.get("sharedContainerRoot").resolve(Directories.CASSANDRA_DATA);

        final Path oldDataFile1 = cassandraDataDir.resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-6-big-Filter.db");
        final Path oldCRCFile1 = cassandraDataDir.resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-6-big-Digest.crc32");
        final Path oldDataFile2 = cassandraDataDir.resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-1-big-Filter.db");
        final Path oldCRCFile2 = cassandraDataDir.resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-1-big-Digest.crc32");
        final Path oldSystemDir = cassandraDataDir.resolve("system");

        Files.createDirectory(oldSystemDir);

        final String oldDataFileText = UUID.randomUUID().toString();
        Files.createDirectories(oldDataFile1.getParent());
        Files.write(oldDataFile1, oldDataFileText.getBytes(), StandardOpenOption.CREATE_NEW);
        Files.write(oldCRCFile1, "123".getBytes(), StandardOpenOption.CREATE_NEW);
        Files.createDirectories(oldDataFile2.getParent());
        Files.write(oldDataFile2, "stuffs".getBytes(), StandardOpenOption.CREATE_NEW);
        Files.write(oldCRCFile2, "234".getBytes(), StandardOpenOption.CREATE_NEW);

        final RestoreArguments arguments = new RestoreArguments(this.getClass().getSimpleName(), null);
        arguments.sourceBackupID = restoreFromNodeId;
        arguments.clusterId = restoreFromClusterId;
        arguments.backupBucket = restoreFromBackupBucket;
        arguments.snapshotTag = tag;

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));

        calendar.set(2017, Calendar.MAY, 2, 2, 6, 0);
        arguments.timestampStart = calendar.getTimeInMillis();
        calendar.set(2017, Calendar.MAY, 2, 2, 7, 0);
        arguments.timestampEnd = calendar.getTimeInMillis();

        Multimap<String, String> keyspaceTablesSubset = HashMultimap.create();
        keyspaceTablesSubset.put("test", "test");

        new RestoreTask(new LocalFileDownloader(tempDirs.get("dummyRemoteSource"), restoreFromNodeId),
                cassandraDataDir,
                tempDirs.get("etc/cassandra"),
                tempDirs.get("sharedContainerRoot"),
                new GlobalLock(tempDirs.get("sharedContainerRoot").toString()),
                arguments,
                keyspaceTablesSubset).call();

        // Confirm SSTables present

        Assert.assertTrue(cassandraDataDir.resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-7-big-Filter.db").toFile().exists());
        Assert.assertTrue(cassandraDataDir.resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-7-big-Statistics.db").toFile().exists());

        Assert.assertFalse(cassandraDataDir.resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-6-big-Filter.db").toFile().exists());
        Assert.assertFalse(cassandraDataDir.resolve("test/test-57c498c01ff511e792786bd71a520b1a/mc-6-big-Digest.crc32").toFile().exists());

        Assert.assertTrue(cassandraDataDir.resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-1-big-Filter.db").toFile().exists());
        Assert.assertTrue(cassandraDataDir.resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-1-big-Digest.crc32").toFile().exists());

        Assert.assertFalse(cassandraDataDir.resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-2-big-Filter.db").toFile().exists());
        Assert.assertFalse(cassandraDataDir.resolve("test_2/test-1101a1b01b5111e792786bd71a520b1a/mc-2-big-Statistics.db").toFile().exists());

        // Confirm cassandra-env.sh present and includes replayList
        final Path cassandraEnv = tempDirs.get("etc/cassandra").resolve("cassandra-env.sh");
        Assert.assertTrue(cassandraEnv.toFile().exists());
        List<String> cassandraEnvText = Files.readAllLines(cassandraEnv);
        Assert.assertTrue(cassandraEnvText.get(0).equals("JVM_OPTS=\"$JVM_OPTS -Dcassandra.replayList=test.test\""));

        // Confirm cassandra.yaml present and includes tokens
        final Path cassandraYaml = tempDirs.get("etc/cassandra").resolve("cassandra.yaml");
        Assert.assertTrue(cassandraYaml.toFile().exists());
        String cassandraYamlText = new String(Files.readAllBytes(cassandraYaml));
        Assert.assertTrue(cassandraYamlText.contains("initial_token: "));
        Assert.assertTrue(cassandraYamlText.contains("auto_bootstrap: false"));
        Assert.assertFalse(cassandraYamlText.contains("auto_bootstrap: true"));
        Assert.assertTrue(cassandraYamlText.contains("cluster_name: alwyn-restore"));
        Assert.assertTrue(cassandraYamlText.contains("server_encryption_options: {keystore: /etc/cassandra/keystore.jks, truststore: /etc/cassandra/truststore.jks"));
    }

    private Set<Path> getFilteredFiles(final String testFile, final Predicate<Path> predicate) throws Exception {
        final Path existingFiles = Paths.get(this.getClass().getClassLoader().getResource(testFile).toURI());
        List<Path> files = Files.readAllLines(existingFiles).stream().map(Paths::get).collect(Collectors.toList());
        return files.stream().filter(predicate).collect(Collectors.toSet());
    }

    @Test(description = "On subset restore, only existing files for subset keypspace.tables should be deleted.")
    public void getDeletableSstablesForSubsetRestore() throws Exception {
        final Set<Path> filteredFiles_3_11 = getFilteredFiles("cassandra/existing_files_3_11.txt", RestorePredicates.isSubsetTable(ImmutableMultimap.of("test", "test2")));

        // Non-subset tables (incl. schema tables) should not be deleted
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/mc-1-big-CompressionInfo.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system/local-7ad54392bcdd35a684174e047860b377/mc-14-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system/local-7ad54392bcdd35a684174e047860b377/mc-14-big-Filter.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/mc-1-big-Index.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system_auth/roles-5bc52802de2535edaeab188eecebb090/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system_schema/aggregates-924c55872e3a345bb10c12f37c1ba895/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/mc-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/mc-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/test/test1-bec2a0308de611e79b78d3373411af00/mc-1-big-Data.db")));

        // Only subset tables should be deleted
        Assert.assertTrue(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/test/test2-c172cc108de611e79b78d3373411af00/mc-1-big-Data.db")));
        Assert.assertTrue(filteredFiles_3_11.contains(Paths.get("/Users/alwyn/.ccm/test_3_11/node1/data/test/test2-c172cc108de611e79b78d3373411af00/mc-1-big-Summary.db")));

        final Set<Path> filteredFiles_3_0 = getFilteredFiles("cassandra/existing_files_3_0.txt", RestorePredicates.isSubsetTable(ImmutableMultimap.of("test", "test2")));

        // Non-subset tables (incl. schema tables) should not be deleted
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/mc-1-big-Digest.crc32")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system/local-7ad54392bcdd35a684174e047860b377/mc-14-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system/local-7ad54392bcdd35a684174e047860b377/mc-14-big-Statistics.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/mc-1-big-Summary.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system_auth/roles-5bc52802de2535edaeab188eecebb090/mc-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system_auth/roles-5bc52802de2535edaeab188eecebb090/mc-1-big-Index.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/mc-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/mc-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/test/test1-df63fad08de811e7a0fd391f1c27ad34/mc-1-big-Data.db")));

        // Only subset tables should be deleted
        Assert.assertTrue(filteredFiles_3_0.contains(Paths.get("/Users/alwyn/.ccm/test_3_0/node1/data/test/test2-e00f7d108de811e7a0fd391f1c27ad34/mc-1-big-Data.db")));

        final Set<Path> filteredFiles_2_2 = getFilteredFiles("cassandra/existing_files_2_2.txt", RestorePredicates.isSubsetTable(ImmutableMultimap.of("test", "test1")));

        // Non-subset tables (incl. schema tables) should not be deleted
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/lb-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/lb-1-big-Index.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data0//system/local-7ad54392bcdd35a684174e047860b377/lb-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data0//system/local-7ad54392bcdd35a684174e047860b377/lb-5-big-Digest.adler32")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/lb-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/lb-1-big-Filter.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/schema_aggregates-a5fc57fc9d6c3bfda3fc01ad54686fea/lb-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/schema_aggregates-a5fc57fc9d6c3bfda3fc01ad54686fea/lb-1-big-Digest.adler32")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/lb-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system/schema_keyspaces-b0f2235744583cdb9631c43e59ce3676/lb-5-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/system_auth/roles-5bc52802de2535edaeab188eecebb090/lb-1-big-Data.db")));
        Assert.assertFalse(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/test/test2-4ecd23a08dea11e787a2ff3655e7a2ae/lb-1-big-Data.db")));

        // Only subset tables should be deleted
        Assert.assertTrue(filteredFiles_2_2.contains(Paths.get("/Users/alwyn/.ccm/test_2_2/node1/data/test/test1-4eb555e08dea11e787a2ff3655e7a2ae/lb-1-big-Data.db")));

        final Set<Path> filteredFiles_2_1 = getFilteredFiles("cassandra/existing_files_2_1.txt", RestorePredicates.isSubsetTable(ImmutableMultimap.of("test", "test1")));

        // System tables on an existing cluster should not be deleted, so tokens / peers aren't lost
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/system-compaction_history-ka-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/system-compaction_history-ka-1-Index.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-10-Data.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/local-7ad54392bcdd35a684174e047860b377/system-local-ka-10-TOC.txt")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/system-peers-ka-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/peers-37f71aca7dc2383ba70672528af04d4f/system-peers-ka-1-Digest.sha1")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/system-schema_columnfamilies-ka-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/system-schema_columnfamilies-ka-1-Index.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/schema_columns-296e9c049bec3085827dc17d3df2122a/system-schema_columns-ka-2-Data.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/system/schema_columns-296e9c049bec3085827dc17d3df2122a/system-schema_columns-ka-2-CompressionInfo.db")));
        Assert.assertFalse(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/test/test2-56b2dbd08dec11e784f7f3682f0fcf14/test-test2-ka-1-Data.db")));

        // All other tables should be candidates for deletion (incl. schema tables in system keyspace)
        Assert.assertTrue(filteredFiles_2_1.contains(Paths.get("/Users/alwyn/.ccm/test_2_1/node1/data/test/test1-567891f08dec11e784f7f3682f0fcf14/test-test1-ka-1-Data.db")));

        final Set<Path> filteredFiles_2_0 = getFilteredFiles("cassandra/existing_files_2_0.txt", RestorePredicates.isSubsetTable(ImmutableMultimap.of("test", "test1")));

        // System tables on an existing cluster should not be deleted, so tokens / peers aren't lost
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/local/system-local-jb-10-Data.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/local/system-local-jb-10-Filter.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/peers/system-peers-jb-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/peers/system-peers-jb-1-CompressionInfo.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/schema_columnfamilies/system-schema_columnfamilies-jb-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/schema_columnfamilies/system-schema_columnfamilies-jb-1-CompressionInfo.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/schema_columns/system-schema_columns-jb-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/schema_columns/system-schema_columns-jb-1-Index.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/system/schema_keyspaces/system-schema_keyspaces-jb-1-Data.db")));
        Assert.assertFalse(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/test/test2/test-test2-jb-1-Data.db")));

        // All other tables should be candidates for deletion (incl. schema tables in system keyspace)
        Assert.assertTrue(filteredFiles_2_0.contains(Paths.get("/Users/alwyn/.ccm/test_2_0/node1/data/test/test1/test-test1-jb-1-Data.db")));
    }

    private List<String> getFilteredManifest(final String testFile, final Predicate<String> predicate) throws Exception {
        final Path manifest = Paths.get(this.getClass().getClassLoader().getResource(testFile).toURI());
        List<String> files = Files.readAllLines(manifest);
        return files.stream().filter(predicate).collect(Collectors.toList());
    }

    @Test(description = "Test getManifestFilesAllExceptSystem (Both full restores on existing and new clusters use this predicate).")
    public void getManifestFilesAllExceptSystem() throws Exception {
        final List<String> filteredManifest_3_11 = getFilteredManifest("cassandra/manifest_3_11.txt", RestorePredicates.getManifestFilesForFullExistingRestore(logger));

        // Download everything except system (non-schema) tables. If restoring to new cluster, then system will be re-created automatically. If restoring to existing cluster,
        // then system already contains token assignments (and we can only restore if PIT and current topology match).
        Assert.assertFalse(filteredManifest_3_11.contains("24 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-2227169002/mc-1-big-Filter.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("450 data/system/sstable_activity-5a1ff267ace03f128563cfae6103c65e/1-491983517/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("5107 data/system/local-7ad54392bcdd35a684174e047860b377/13-2644929720/mc-13-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("50 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-3111710351/mc-2-big-Data.db"));

        Assert.assertTrue(filteredManifest_3_11.contains("89 data/instaclustr/recovery_codes-43fa5ca08df311e788dbd999c5a6d937/1-1355695387/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("33 data/test/test2-7789ac108df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("92 data/test/test2-7789ac108df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-TOC.txt"));
        Assert.assertTrue(filteredManifest_3_11.contains("33 data/test/test1-7738ccf08df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("43 data/test/test1-7738ccf08df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("174 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-2451342696/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("65 data/system_schema/tables-afddfb9dbc1e30688056eed6c302ba09/11-528025750/mc-11-big-Summary.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("3033 data/system_schema/tables-afddfb9dbc1e30688056eed6c302ba09/11-528025750/mc-11-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("121 data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/12-2712839474/mc-12-big-Data.db"));

        final List<String> filteredManifest_3_0 = getFilteredManifest("cassandra/manifest_3_0.txt", RestorePredicates.getManifestFilesForFullExistingRestore(logger));

        // Download everything except system (non-schema) tables. If restoring to new cluster, then system will be re-created automatically. If restoring to existing cluster,
        // then system already contains token assignments (and we can only restore if PIT and current topology match).
        Assert.assertFalse(filteredManifest_3_0.contains("217 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-4030006069/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("43 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-4030006069/mc-1-big-CompressionInfo.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("5099 data/system/local-7ad54392bcdd35a684174e047860b377/13-1621859795/mc-13-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("50 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-1117389354/mc-2-big-Data.db"));

        Assert.assertTrue(filteredManifest_3_0.contains("184 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-1050904704/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("68 data/system_auth/resource_role_permissons_index-5f2fbdad91f13946bd25d5da3a5c35ec/1-3137044675/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("51 data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/9-1643706196/mc-9-big-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("51 data/system_schema/views-9786ac1cdd583201a7cdad556410c985/1-455380729/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("27 data/system_schema/triggers-4df70b666b05325195a132b54005fd48/1-455380729/mc-1-big-Index.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("16 data/instaclustr/recovery_codes-ffbbdd708df711e7959847bfbbe11be0/1-2610346153/mc-1-big-Filter.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("33 data/test/test1-ff973e708df711e7959847bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("33 data/test/test2-004be8708df811e7959847bfbbe11be0/1-1828863006/mc-1-big-Data.db"));

        final List<String> filteredManifest_2_2 = getFilteredManifest("cassandra/manifest_2_2.txt", RestorePredicates.getManifestFilesForFullExistingRestore(logger));

        // Download everything except system (non-schema) tables. If restoring to new cluster, then system will be re-created automatically. If restoring to existing cluster,
        // then system already contains token assignments (and we can only restore if PIT and current topology match).
        Assert.assertFalse(filteredManifest_2_2.contains("926 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-1674037240/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("4665 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-1674037240/lb-1-big-Statistics.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("326 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-2725471003/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("5797 data/system/local-7ad54392bcdd35a684174e047860b377/5-4132450163/lb-5-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("578 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-3748469067/lb-2-big-Data.db"));

        Assert.assertTrue(filteredManifest_2_2.contains("255 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-4192292291/lb-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("16 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-4192292291/lb-1-big-Filter.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("9706 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/9-3053070001/lb-9-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("84 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/10-1768680026/lb-10-big-Summary.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("20 data/system/schema_triggers-0359bc7171233ee19a4ab9dfb11fc125/1-2380859841/lb-1-big-Index.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("1722 data/instaclustr/sla_latency-88de98108df611e7809ed92170ad3012/1-657431499/lb-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("41 data/test/test1-894597408df611e7984c4f3c8ea79bea/1-3349088670/lb-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("43 data/test/test1-894597408df611e7984c4f3c8ea79bea/1-3349088670/lb-1-big-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("41 data/test/test2-89d8af808df611e7984c4f3c8ea79bea/1-3406432793/lb-1-big-Data.db"));

        final List<String> filteredManifest_2_1 = getFilteredManifest("cassandra/manifest_2_1.txt", RestorePredicates.getManifestFilesForFullExistingRestore(logger));

        // Download everything except system (non-schema) tables. If restoring to new cluster, then system will be re-created automatically. If restoring to existing cluster,
        // then system already contains token assignments (and we can only restore if PIT and current topology match).
        Assert.assertFalse(filteredManifest_2_1.contains("16 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-2300863697/system-paxos-ka-1-Filter.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("101 data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/1-4223796063/system-size_estimates-ka-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("5748 data/system/local-7ad54392bcdd35a684174e047860b377/5-1627205357/system-local-ka-5-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("9 data/system/peers-37f71aca7dc2383ba70672528af04d4f/1-674356819/system-peers-ka-1-Digest.sha1"));
        Assert.assertFalse(filteredManifest_2_1.contains("79 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-331881221/system-peers-ka-2-Data.db"));

        Assert.assertTrue(filteredManifest_2_1.contains("91 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/11-4262068625/system-schema_columnfamilies-ka-11-TOC.txt"));
        Assert.assertTrue(filteredManifest_2_1.contains("43 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/10-3961258184/system-schema_columnfamilies-ka-10-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("91 data/system/schema_keyspaces-b0f2235744583cdb9631c43e59ce3676/15-652883343/system-schema_keyspaces-ka-15-TOC.txt"));
        Assert.assertTrue(filteredManifest_2_1.contains("12555 data/system/schema_columns-296e9c049bec3085827dc17d3df2122a/9-1321408867/system-schema_columns-ka-9-Data.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("126 data/instaclustr/recovery_codes-f1e51ee08df811e79a1f477c66bf43d4/1-3842320373/instaclustr-recovery_codes-ka-1-Data.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("41 data/test/test1-fc0940408df811e79a1f477c66bf43d4/1-3346794853/test-test1-ka-1-Data.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("16 data/test/test1-fc0940408df811e79a1f477c66bf43d4/1-3346794853/test-test1-ka-1-Filter.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("41 data/test/test2-fca818508df811e79a1f477c66bf43d4/1-3374844368/test-test2-ka-1-Data.db"));

        final List<String> filteredManifest_2_0 = getFilteredManifest("cassandra/manifest_2_0.txt", RestorePredicates.getManifestFilesForFullExistingRestore(logger));

        // Download everything except system (non-schema) tables. If restoring to new cluster, then system will be re-created automatically. If restoring to existing cluster,
        // then system already contains token assignments (and we can only restore if PIT and current topology match).
        Assert.assertFalse(filteredManifest_2_0.contains("116 data/system/compaction_history/1-4017759835/system-compaction_history-jb-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("116 data/system/paxos/1-2896914298/system-paxos-jb-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("5498 data/system/peers/2-3987263071/system-peers-jb-2-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("80 data/system/local/6-3908442648/system-local-jb-6-Data.db"));

        Assert.assertTrue(filteredManifest_2_0.contains("43 data/system/schema_triggers/1-2476738957/system-schema_triggers-jb-1-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("8417 data/system/schema_columnfamilies/13-3926797722/system-schema_columnfamilies-jb-13-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("336 data/system/schema_keyspaces/17-3369950948/system-schema_keyspaces-jb-17-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("900 data/instaclustr/sla_latency/1-503989699/instaclustr-sla_latency-jb-1-Index.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("41 data/test/test1/1-3114272916/test-test1-jb-1-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("18 data/test/test1/1-3114272916/test-test1-jb-1-Index.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("41 data/test/test2/1-3027896432/test-test2-jb-1-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("16 data/test/test2/1-3027896432/test-test2-jb-1-Filter.db"));
    }

    @Test(description = "Only download keyspace.table files specified in the subset paramter.")
    public void getManifestFilesForSubsetExistingRestore() throws Exception {
        final List<String> filteredManifest_3_11 = getFilteredManifest("cassandra/manifest_3_11.txt", RestorePredicates.getManifestFilesForSubsetExistingRestore(logger, ImmutableMultimap.of("test", "test1")));

        Assert.assertFalse(filteredManifest_3_11.contains("24 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-2227169002/mc-1-big-Filter.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("450 data/system/sstable_activity-5a1ff267ace03f128563cfae6103c65e/1-491983517/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("5107 data/system/local-7ad54392bcdd35a684174e047860b377/13-2644929720/mc-13-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("50 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-3111710351/mc-2-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("89 data/instaclustr/recovery_codes-43fa5ca08df311e788dbd999c5a6d937/1-1355695387/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("33 data/test/test2-7789ac108df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("92 data/test/test2-7789ac108df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-TOC.txt"));
        Assert.assertFalse(filteredManifest_3_11.contains("174 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-2451342696/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("65 data/system_schema/tables-afddfb9dbc1e30688056eed6c302ba09/11-528025750/mc-11-big-Summary.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("3033 data/system_schema/tables-afddfb9dbc1e30688056eed6c302ba09/11-528025750/mc-11-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("121 data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/12-2712839474/mc-12-big-Data.db"));

        Assert.assertTrue(filteredManifest_3_11.contains("33 data/test/test1-7738ccf08df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("43 data/test/test1-7738ccf08df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-CompressionInfo.db"));

        final List<String> filteredManifest_3_0 = getFilteredManifest("cassandra/manifest_3_0.txt", RestorePredicates.getManifestFilesForSubsetExistingRestore(logger, ImmutableMultimap.of("test", "test1")));

        Assert.assertFalse(filteredManifest_3_0.contains("217 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-4030006069/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("43 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-4030006069/mc-1-big-CompressionInfo.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("5099 data/system/local-7ad54392bcdd35a684174e047860b377/13-1621859795/mc-13-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("50 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-1117389354/mc-2-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("184 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-1050904704/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("68 data/system_auth/resource_role_permissons_index-5f2fbdad91f13946bd25d5da3a5c35ec/1-3137044675/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("51 data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/9-1643706196/mc-9-big-CompressionInfo.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("51 data/system_schema/views-9786ac1cdd583201a7cdad556410c985/1-455380729/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("27 data/system_schema/triggers-4df70b666b05325195a132b54005fd48/1-455380729/mc-1-big-Index.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("16 data/instaclustr/recovery_codes-ffbbdd708df711e7959847bfbbe11be0/1-2610346153/mc-1-big-Filter.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("33 data/test/test2-004be8708df811e7959847bfbbe11be0/1-1828863006/mc-1-big-Data.db"));

        Assert.assertTrue(filteredManifest_3_0.contains("33 data/test/test1-ff973e708df711e7959847bfbbe11be0/1-1828863006/mc-1-big-Data.db"));

        final List<String> filteredManifest_2_2 = getFilteredManifest("cassandra/manifest_2_2.txt", RestorePredicates.getManifestFilesForSubsetExistingRestore(logger, ImmutableMultimap.of("test", "test1")));

        Assert.assertFalse(filteredManifest_2_2.contains("926 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-1674037240/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("4665 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-1674037240/lb-1-big-Statistics.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("326 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-2725471003/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("5797 data/system/local-7ad54392bcdd35a684174e047860b377/5-4132450163/lb-5-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("578 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-3748469067/lb-2-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("255 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-4192292291/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("16 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-4192292291/lb-1-big-Filter.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("9706 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/9-3053070001/lb-9-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("84 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/10-1768680026/lb-10-big-Summary.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("20 data/system/schema_triggers-0359bc7171233ee19a4ab9dfb11fc125/1-2380859841/lb-1-big-Index.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("1722 data/instaclustr/sla_latency-88de98108df611e7809ed92170ad3012/1-657431499/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("41 data/test/test2-89d8af808df611e7984c4f3c8ea79bea/1-3406432793/lb-1-big-Data.db"));

        Assert.assertTrue(filteredManifest_2_2.contains("41 data/test/test1-894597408df611e7984c4f3c8ea79bea/1-3349088670/lb-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("43 data/test/test1-894597408df611e7984c4f3c8ea79bea/1-3349088670/lb-1-big-CompressionInfo.db"));

        final List<String> filteredManifest_2_1 = getFilteredManifest("cassandra/manifest_2_1.txt", RestorePredicates.getManifestFilesForSubsetExistingRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_2_1.contains("16 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-2300863697/system-paxos-ka-1-Filter.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("101 data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/1-4223796063/system-size_estimates-ka-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("5748 data/system/local-7ad54392bcdd35a684174e047860b377/5-1627205357/system-local-ka-5-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("9 data/system/peers-37f71aca7dc2383ba70672528af04d4f/1-674356819/system-peers-ka-1-Digest.sha1"));
        Assert.assertFalse(filteredManifest_2_1.contains("79 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-331881221/system-peers-ka-2-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("91 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/11-4262068625/system-schema_columnfamilies-ka-11-TOC.txt"));
        Assert.assertFalse(filteredManifest_2_1.contains("43 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/10-3961258184/system-schema_columnfamilies-ka-10-CompressionInfo.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("91 data/system/schema_keyspaces-b0f2235744583cdb9631c43e59ce3676/15-652883343/system-schema_keyspaces-ka-15-TOC.txt"));
        Assert.assertFalse(filteredManifest_2_1.contains("12555 data/system/schema_columns-296e9c049bec3085827dc17d3df2122a/9-1321408867/system-schema_columns-ka-9-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("126 data/instaclustr/recovery_codes-f1e51ee08df811e79a1f477c66bf43d4/1-3842320373/instaclustr-recovery_codes-ka-1-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("41 data/test/test1-fc0940408df811e79a1f477c66bf43d4/1-3346794853/test-test1-ka-1-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("16 data/test/test1-fc0940408df811e79a1f477c66bf43d4/1-3346794853/test-test1-ka-1-Filter.db"));

        Assert.assertTrue(filteredManifest_2_1.contains("41 data/test/test2-fca818508df811e79a1f477c66bf43d4/1-3374844368/test-test2-ka-1-Data.db"));

        final List<String> filteredManifest_2_0 = getFilteredManifest("cassandra/manifest_2_0.txt", RestorePredicates.getManifestFilesForSubsetExistingRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_2_0.contains("116 data/system/compaction_history/1-4017759835/system-compaction_history-jb-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("116 data/system/paxos/1-2896914298/system-paxos-jb-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("5498 data/system/peers/2-3987263071/system-peers-jb-2-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("80 data/system/local/6-3908442648/system-local-jb-6-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("43 data/system/schema_triggers/1-2476738957/system-schema_triggers-jb-1-CompressionInfo.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("8417 data/system/schema_columnfamilies/13-3926797722/system-schema_columnfamilies-jb-13-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("336 data/system/schema_keyspaces/17-3369950948/system-schema_keyspaces-jb-17-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("900 data/instaclustr/sla_latency/1-503989699/instaclustr-sla_latency-jb-1-Index.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("41 data/test/test1/1-3114272916/test-test1-jb-1-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("18 data/test/test1/1-3114272916/test-test1-jb-1-Index.db"));

        Assert.assertTrue(filteredManifest_2_0.contains("41 data/test/test2/1-3027896432/test-test2-jb-1-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("16 data/test/test2/1-3027896432/test-test2-jb-1-Filter.db"));
    }

    @Test(description = "Download keyspace.table files specified in the subset parameter and also system_auth and schema tables.")
    public void getManifestFilesForSubsetNewRestore() throws Exception {
        final List<String> filteredManifest_3_11 = getFilteredManifest("cassandra/manifest_3_11.txt", RestorePredicates.getManifestFilesForSubsetNewRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_3_11.contains("24 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-2227169002/mc-1-big-Filter.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("450 data/system/sstable_activity-5a1ff267ace03f128563cfae6103c65e/1-491983517/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("5107 data/system/local-7ad54392bcdd35a684174e047860b377/13-2644929720/mc-13-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("50 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-3111710351/mc-2-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("89 data/instaclustr/recovery_codes-43fa5ca08df311e788dbd999c5a6d937/1-1355695387/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("33 data/test/test1-7738ccf08df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_11.contains("43 data/test/test1-7738ccf08df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-CompressionInfo.db"));

        Assert.assertTrue(filteredManifest_3_11.contains("65 data/system_schema/tables-afddfb9dbc1e30688056eed6c302ba09/11-528025750/mc-11-big-Summary.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("3033 data/system_schema/tables-afddfb9dbc1e30688056eed6c302ba09/11-528025750/mc-11-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("121 data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/12-2712839474/mc-12-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("174 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-2451342696/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("33 data/test/test2-7789ac108df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_11.contains("92 data/test/test2-7789ac108df311e7aa5047bfbbe11be0/1-1828863006/mc-1-big-TOC.txt"));

        final List<String> filteredManifest_3_0 = getFilteredManifest("cassandra/manifest_3_0.txt", RestorePredicates.getManifestFilesForSubsetNewRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_3_0.contains("217 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-4030006069/mc-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("43 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-4030006069/mc-1-big-CompressionInfo.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("5099 data/system/local-7ad54392bcdd35a684174e047860b377/13-1621859795/mc-13-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("50 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-1117389354/mc-2-big-Data.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("16 data/instaclustr/recovery_codes-ffbbdd708df711e7959847bfbbe11be0/1-2610346153/mc-1-big-Filter.db"));
        Assert.assertFalse(filteredManifest_3_0.contains("33 data/test/test1-ff973e708df711e7959847bfbbe11be0/1-1828863006/mc-1-big-Data.db"));

        Assert.assertTrue(filteredManifest_3_0.contains("51 data/system_schema/keyspaces-abac5682dea631c5b535b3d6cffd0fb6/9-1643706196/mc-9-big-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("51 data/system_schema/views-9786ac1cdd583201a7cdad556410c985/1-455380729/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("27 data/system_schema/triggers-4df70b666b05325195a132b54005fd48/1-455380729/mc-1-big-Index.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("184 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-1050904704/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("68 data/system_auth/resource_role_permissons_index-5f2fbdad91f13946bd25d5da3a5c35ec/1-3137044675/mc-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_3_0.contains("33 data/test/test2-004be8708df811e7959847bfbbe11be0/1-1828863006/mc-1-big-Data.db"));

        final List<String> filteredManifest_2_2 = getFilteredManifest("cassandra/manifest_2_2.txt", RestorePredicates.getManifestFilesForSubsetNewRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_2_2.contains("926 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-1674037240/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("4665 data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/1-1674037240/lb-1-big-Statistics.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("326 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-2725471003/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("5797 data/system/local-7ad54392bcdd35a684174e047860b377/5-4132450163/lb-5-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("578 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-3748469067/lb-2-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("1722 data/instaclustr/sla_latency-88de98108df611e7809ed92170ad3012/1-657431499/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("41 data/test/test1-894597408df611e7984c4f3c8ea79bea/1-3349088670/lb-1-big-Data.db"));
        Assert.assertFalse(filteredManifest_2_2.contains("43 data/test/test1-894597408df611e7984c4f3c8ea79bea/1-3349088670/lb-1-big-CompressionInfo.db"));

        Assert.assertTrue(filteredManifest_2_2.contains("9706 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/9-3053070001/lb-9-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("84 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/10-1768680026/lb-10-big-Summary.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("20 data/system/schema_triggers-0359bc7171233ee19a4ab9dfb11fc125/1-2380859841/lb-1-big-Index.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("255 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-4192292291/lb-1-big-Data.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("16 data/system_auth/roles-5bc52802de2535edaeab188eecebb090/1-4192292291/lb-1-big-Filter.db"));
        Assert.assertTrue(filteredManifest_2_2.contains("41 data/test/test2-89d8af808df611e7984c4f3c8ea79bea/1-3406432793/lb-1-big-Data.db"));

        final List<String> filteredManifest_2_1 = getFilteredManifest("cassandra/manifest_2_1.txt", RestorePredicates.getManifestFilesForSubsetNewRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_2_1.contains("16 data/system/paxos-b7b7f0c2fd0a34108c053ef614bb7c2d/1-2300863697/system-paxos-ka-1-Filter.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("101 data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/1-4223796063/system-size_estimates-ka-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("5748 data/system/local-7ad54392bcdd35a684174e047860b377/5-1627205357/system-local-ka-5-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("9 data/system/peers-37f71aca7dc2383ba70672528af04d4f/1-674356819/system-peers-ka-1-Digest.sha1"));
        Assert.assertFalse(filteredManifest_2_1.contains("79 data/system/peers-37f71aca7dc2383ba70672528af04d4f/2-331881221/system-peers-ka-2-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("126 data/instaclustr/recovery_codes-f1e51ee08df811e79a1f477c66bf43d4/1-3842320373/instaclustr-recovery_codes-ka-1-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("41 data/test/test1-fc0940408df811e79a1f477c66bf43d4/1-3346794853/test-test1-ka-1-Data.db"));
        Assert.assertFalse(filteredManifest_2_1.contains("16 data/test/test1-fc0940408df811e79a1f477c66bf43d4/1-3346794853/test-test1-ka-1-Filter.db"));

        Assert.assertTrue(filteredManifest_2_1.contains("91 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/11-4262068625/system-schema_columnfamilies-ka-11-TOC.txt"));
        Assert.assertTrue(filteredManifest_2_1.contains("43 data/system/schema_columnfamilies-45f5b36024bc3f83a3631034ea4fa697/10-3961258184/system-schema_columnfamilies-ka-10-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("91 data/system/schema_keyspaces-b0f2235744583cdb9631c43e59ce3676/15-652883343/system-schema_keyspaces-ka-15-TOC.txt"));
        Assert.assertTrue(filteredManifest_2_1.contains("12555 data/system/schema_columns-296e9c049bec3085827dc17d3df2122a/9-1321408867/system-schema_columns-ka-9-Data.db"));
        Assert.assertTrue(filteredManifest_2_1.contains("41 data/test/test2-fca818508df811e79a1f477c66bf43d4/1-3374844368/test-test2-ka-1-Data.db"));

        final List<String> filteredManifest_2_0 = getFilteredManifest("cassandra/manifest_2_0.txt", RestorePredicates.getManifestFilesForSubsetNewRestore(logger, ImmutableMultimap.of("test", "test2")));

        Assert.assertFalse(filteredManifest_2_0.contains("116 data/system/compaction_history/1-4017759835/system-compaction_history-jb-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("116 data/system/paxos/1-2896914298/system-paxos-jb-1-Summary.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("5498 data/system/peers/2-3987263071/system-peers-jb-2-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("80 data/system/local/6-3908442648/system-local-jb-6-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("900 data/instaclustr/sla_latency/1-503989699/instaclustr-sla_latency-jb-1-Index.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("41 data/test/test1/1-3114272916/test-test1-jb-1-Data.db"));
        Assert.assertFalse(filteredManifest_2_0.contains("18 data/test/test1/1-3114272916/test-test1-jb-1-Index.db"));

        Assert.assertTrue(filteredManifest_2_0.contains("43 data/system/schema_triggers/1-2476738957/system-schema_triggers-jb-1-CompressionInfo.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("8417 data/system/schema_columnfamilies/13-3926797722/system-schema_columnfamilies-jb-13-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("336 data/system/schema_keyspaces/17-3369950948/system-schema_keyspaces-jb-17-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("41 data/test/test2/1-3027896432/test-test2-jb-1-Data.db"));
        Assert.assertTrue(filteredManifest_2_0.contains("16 data/test/test2/1-3027896432/test-test2-jb-1-Filter.db"));
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp () throws IOException {
        testHelperService.deleteTempDirectories(tempDirs);
    }
}