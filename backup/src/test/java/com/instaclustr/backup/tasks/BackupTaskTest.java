package com.instaclustr.backup.tasks;

import com.google.common.collect.*;
import com.instaclustr.backup.BackupArguments;
import com.instaclustr.backup.RestoreArguments;
import com.instaclustr.backup.downloader.LocalFileDownloader;
import com.instaclustr.backup.jmx.CassandraVersion;
import com.instaclustr.backup.service.TestHelperService;
import com.instaclustr.backup.task.BackupTask;
import com.instaclustr.backup.task.ManifestEntry;
import com.instaclustr.backup.task.RestoreTask;
import com.instaclustr.backup.util.Directories;
import com.instaclustr.backup.util.GlobalLock;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class BackupTaskTest {
    private final String sha1Hash = "3a1bd6900872256303b1ed036881cd35f5b670ce";
    private String testSnapshotName = "testSnapshot";
    // Adler32 computed by python
    // zlib.adler32("dnvbjaksdbhr7239iofhusdkjfhgkauyg83uhdjshkusdhoryhjzdgfk8ei") & 0xffffffff -> 2973505342
    private final byte[] testData = "dnvbjaksdbhr7239iofhusdkjfhgkauyg83uhdjshkusdhoryhjzdgfk8ei".getBytes();
    private static final String nodeId = "DUMMY_NODE_ID";
    private static final String clusterId = "DUMMY_CLUSTER_ID";
    private static final String backupBucket = Optional.ofNullable(System.getenv("TEST_BUCKET")).orElse("fooo");
    private final Long independentChecksum = 2973505342L;
    private List<String> tokens = ImmutableList.of("1", "2", "3", "4", "5");

    private class TestFileConfig {
        final CassandraVersion cassandraVersion;
        final String sstableVersion;

        TestFileConfig(final CassandraVersion cassandraVersion) {
            this.cassandraVersion = cassandraVersion;

            if (cassandraVersion == CassandraVersion.THREE) {
                this.sstableVersion = "mb";
            } else if (cassandraVersion == CassandraVersion.TWO_TWO) {
                this.sstableVersion = "lb";
            } else {
                this.sstableVersion = "jb";
            }
        }

        String getSstablePrefix(final String keyspace, final String table) {
            if (this.cassandraVersion == CassandraVersion.TWO_ZERO) {
                return String.format("%s-%s-%s", keyspace, table, this.sstableVersion);
            }

            return this.sstableVersion;
        }

        String getChecksum(final String keyspace, final String table) {
            if (this.cassandraVersion == CassandraVersion.TWO_ZERO) {
                return String.format("%s  %s-%s-%s-1-Data.db", sha1Hash, keyspace, table, this.sstableVersion);
            }

            // 2.1 sha1 contains just checksum (compressed and uncompressed)
            if (this.cassandraVersion == CassandraVersion.TWO_ONE) {
                return sha1Hash;
            }

            // 2.2 adler32 contains just checksum (compressed and uncompressed)
            // 3.0 and 3.1 crc32 contains just checksum (compressed and uncompressed)
            return Long.toString(1000000000L);
        }

        // 2.0 only creates digest files for un-compressed SSTables
        boolean createDigest(final boolean isCompressed) {
            return cassandraVersion != CassandraVersion.TWO_ZERO || !isCompressed;
        }
    }

    private final List<TestFileConfig> versionsToTest = ImmutableList.of(
            new TestFileConfig(CassandraVersion.TWO_ZERO),
            new TestFileConfig(CassandraVersion.TWO_TWO),
            new TestFileConfig(CassandraVersion.THREE)
    );

    private static final Map<String, Path> tempDirs = new LinkedHashMap<>();

    static private final ImmutableList<String> SSTABLE_FILES = ImmutableList.of(
            "TOC.txt", "CompressionInfo.db", "Data.db", "Filter.db", "Index.db", "Statistics.db", "Summary.db");

    private final TestHelperService testHelperService = new TestHelperService();

    private void createSSTable(final Path folder, final String keyspace, final String table, final int sequence, final TestFileConfig testFileConfig, final boolean isCompressed, final String tag) throws IOException {

        final Path ssTablePath = folder.resolve(keyspace).resolve(table);
        final Path ssTableSnapshotPath = folder.resolve(keyspace).resolve(table).resolve("snapshots").resolve(tag);
        Files.createDirectories(ssTablePath);
        Files.createDirectories(ssTableSnapshotPath);

        if (testFileConfig.createDigest(isCompressed)) {
            final Path digest = ssTablePath.resolve(String.format("%s-%s-big-Digest.crc32", testFileConfig.getSstablePrefix(keyspace, table), sequence));
            try (final Writer writer = Files.newBufferedWriter(digest)) {
                writer.write(testFileConfig.getChecksum(keyspace, table));
            }
            if(tag != null)
                Files.copy(digest, ssTableSnapshotPath.resolve(digest.getFileName()));
        }

        for (String name : SSTABLE_FILES) {
            final Path path = ssTablePath.resolve(String.format("%s-%s-big-%s", testFileConfig.getSstablePrefix(keyspace, table), sequence, name));
            Files.createFile(path);
            Files.write(path, testData);
            if(tag != null)
                Files.copy(path, ssTableSnapshotPath.resolve(path.getFileName()));
        }

    }

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        for (TestFileConfig testFileConfig : versionsToTest) {
            tempDirs.put(testFileConfig.cassandraVersion.name(), null);
            tempDirs.put(testFileConfig.cassandraVersion.name() + "/data", null);
            tempDirs.put(testFileConfig.cassandraVersion.name() + "/hints", null);
            tempDirs.put(testFileConfig.cassandraVersion.name() + "/etc/cassandra", null);
        }

        testHelperService.setupTempDirectories(tempDirs);

        resetDirectories();
    }

    private void clearDirs() throws IOException {
//        Iterator<Map.Entry<String, Path>> tempDirsIterator = tempDirs.entrySet().iterator();
        tempDirs.entrySet().stream().forEach(entry -> {
            if (entry.getKey().equals("dummyRemoteSource")) {
                return;
            }

            try {
                FileUtils.deleteDirectory(entry.getValue().toFile());
                Files.createDirectories(entry.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

//    @BeforeMethod()
    public void resetDirectories() throws IOException {

        clearDirs();

        for (TestFileConfig testFileConfig : versionsToTest) {
            final Path root = tempDirs.get(testFileConfig.cassandraVersion.name());
            final Path data = root.resolve("data/");
            final String keyspace = "keyspace1";
            final String table1 = "table1";
            createSSTable(data, keyspace, table1, 1, testFileConfig, false, testSnapshotName);
            createSSTable(data, keyspace, table1, 2, testFileConfig, true, testSnapshotName);
            createSSTable(data, keyspace, table1, 3, testFileConfig, true, testSnapshotName);

            final String table2 = "table2";
            createSSTable(data, keyspace, table2, 1, testFileConfig, true, testSnapshotName);
            createSSTable(data, keyspace, table2, 2, testFileConfig, true, testSnapshotName);

        }
    }

    private Set<Path> getFilteredFiles(final String testFile, final Predicate<Path> predicate) throws Exception {
        final Path existingFiles = Paths.get(this.getClass().getClassLoader().getResource(testFile).toURI());
        List<Path> files = Files.readAllLines(existingFiles).stream().map(Paths::get).collect(Collectors.toList());
        return files.stream().filter(predicate).collect(Collectors.toSet());
    }

    private List<String> getFilteredManifest(final String testFile, final Predicate<String> predicate) throws Exception {
        final Path manifest = Paths.get(this.getClass().getClassLoader().getResource(testFile).toURI());
        List<String> files = Files.readAllLines(manifest);
        return files.stream().filter(predicate).collect(Collectors.toList());
    }

    private List<Path> resolveSSTableComponentPaths(final String keyspace, final String table, final Path cassandraRoot, final int sequence, final TestFileConfig testFileConfig) {
        return SSTABLE_FILES.stream()
                .map(name -> cassandraRoot.resolve(Directories.CASSANDRA_DATA)
                        .resolve(keyspace)
                        .resolve(table)
                        .resolve(String.format("%s-%s-big-%s", testFileConfig.getSstablePrefix(keyspace, table) ,sequence, name)))
                .collect(Collectors.toList());
    }


    @Test(description = "Full backup and restore to an existing cluster")
    public void basicRestore() throws Exception {
        for (TestFileConfig testFileConfig : versionsToTest) {
            final Path sharedContainerRoot = tempDirs.get(testFileConfig.cassandraVersion.name());
            final File manifestFile = new File(sharedContainerRoot.resolve("manifests/" + testSnapshotName).toString());

            final List<String> rawBaseArguments = ImmutableList.of(
                    "--bs", "GCP_BLOB",
                    "--dd", sharedContainerRoot.toString(),
                    "--cd", sharedContainerRoot.resolve("etc/cassandra").toString(),
                    "-p", sharedContainerRoot.toString()
            );

            final List<String> rawCommonBackupArguments = ImmutableList.of(
//                    "-j", "JMXxxxxx",
                    "--bucket", backupBucket,
                    "--id", nodeId
            );
            final List<String> rawBackupArguments = ImmutableList.of(
                    "-t", testSnapshotName,
                    "--offline", "true"
            );

            final List<String> rawRestoreArguments = ImmutableList.of(
                    "-bi", nodeId,
                    "-c", clusterId,
                    "-bb", backupBucket,
                    "-s", testSnapshotName

//                    "-j", "JMXxxxxx"
            );

            final BackupArguments backupArguments = new BackupArguments("cassandra-test", System.err);
            backupArguments.parseArguments(Stream.of(rawBaseArguments, rawCommonBackupArguments, rawBackupArguments)
                    .flatMap(List::stream)
                    .toArray(String[]::new));


            final RestoreArguments restoreArguments = new RestoreArguments("cassandra-restore", System.err);
            restoreArguments.parseArguments(Stream.of(rawBaseArguments,rawRestoreArguments)
                    .flatMap(List::stream)
                    .toArray(String[]::new));

            final Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("GMT"));

            calendar.set(2017, Calendar.MAY, 2, 2, 6, 0);
            restoreArguments.timestampStart = calendar.getTimeInMillis();
            calendar.set(2017, Calendar.MAY, 2, 2, 7, 0);
            restoreArguments.timestampEnd = calendar.getTimeInMillis();


            new BackupTask(backupArguments, new GlobalLock(sharedContainerRoot.toString())).call();

            clearDirs();

            //Make sure we deleted the files
            String keyspace = "keyspace1";
            String table = "table1";

            resolveSSTableComponentPaths(keyspace, table, sharedContainerRoot, 1, testFileConfig).stream()
                    .map(Path::toFile)
                    .map(File::exists)
                    .forEach(Assert::assertFalse);


            new RestoreTask(new LocalFileDownloader(tempDirs.get("dummyRemoteSource"), nodeId),
                    sharedContainerRoot,
                    sharedContainerRoot.resolve("etc/cassandra"),
                    sharedContainerRoot,
                    new GlobalLock(sharedContainerRoot.toString()),
                    restoreArguments,
                    HashMultimap.create()).call();

            // Confirm manifest downloaded
            Assert.assertTrue(manifestFile.exists());

            Stream.of(1,2,3).forEach(sequence ->
                resolveSSTableComponentPaths(keyspace, table, sharedContainerRoot, sequence, testFileConfig).stream()
                        .map(Path::toFile)
                        .map(File::exists)
                        .forEach(Assert::assertFalse));

            // Confirm cassandra.yaml present and includes tokens
            final Path cassandraYaml = sharedContainerRoot.resolve("etc/cassandra").resolve("cassandra.yaml");
            Assert.assertTrue(cassandraYaml.toFile().exists());
            String cassandraYamlText = new String(Files.readAllBytes(cassandraYaml));
            Assert.assertTrue(cassandraYamlText.contains("initial_token: "));
            Assert.assertTrue(cassandraYamlText.contains("auto_bootstrap: false"));
            Assert.assertFalse(cassandraYamlText.contains("auto_bootstrap: true"));
            Assert.assertTrue(cassandraYamlText.contains("cluster_name: alwyn-restore"));
            Assert.assertTrue(cassandraYamlText.contains("server_encryption_options: {keystore: /etc/cassandra/keystore.jks, truststore: /etc/cassandra/truststore.jks"));

        }
    }





    @Test(description = "Check that we are checksumming properly")
    public void testCalculateDigest() throws Exception {
        for (TestFileConfig testFileConfig : versionsToTest) {
            final String keyspace = "keyspace1";
            final String table1 = "table1";
            final Path table1Path = tempDirs.get(testFileConfig.cassandraVersion.name()).resolve("data/" + keyspace + "/" + table1);
            final Path path = table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)));
            final String checksum = BackupTask.calculateChecksum(path);
            Assert.assertEquals(checksum, String.valueOf(independentChecksum));
        }
    }

    @Test(description = " Test Manifest Method")
    public void testGenerateManifest() throws Exception {
        for (TestFileConfig testFileConfig: versionsToTest) {
            final Path root = tempDirs.get(testFileConfig.cassandraVersion.name());
            final Path path = root.resolve("data/");
            final List<String> keyspaces = ImmutableList.of("keyspace1");
            final List<String> tables = ImmutableList.of("table1");
            Collection<ManifestEntry> manifestEntries = BackupTask.generateManifest(keyspaces, testSnapshotName, path);

            //Make sure all components of the SSTable are in it.
            for (String name : SSTABLE_FILES) {
                Path expected = tempDirs.get(testFileConfig.cassandraVersion.name()).resolve("data/keyspace1/table1").resolve(String.format("%s-%s-big-%s", testFileConfig.getSstablePrefix("keyspace1", "table1"), 1, name));
                Assert.assertEquals(manifestEntries.stream()
                        .anyMatch(x -> Paths.get("/").resolve(x.objectKey.subpath(0, x.objectKey.getNameCount() - 2).resolve(x.objectKey.getFileName())).compareTo(expected) == 0), true);

            }
        }
    }

    @Test(description = "Test that the manifest is correctly constructed, includes expected files and generates checksum if necessary")
    public void testSSTableLister() throws Exception {
        for (TestFileConfig testFileConfig : versionsToTest) {
            Path backupRoot = Paths.get("/backupRoot/keyspace1");

            final String keyspace = "keyspace1";
            final String table1 = "table1";
            final Path table1Path = tempDirs.get(testFileConfig.cassandraVersion.name()).resolve("data/" + keyspace + "/" + table1);
            Collection<ManifestEntry> manifest = BackupTask.ssTableManifest(table1Path, backupRoot.resolve(table1Path.getFileName()));

            final String table2 = "table2";
            final Path table2Path = tempDirs.get(testFileConfig.cassandraVersion.name()).resolve("data/" + keyspace + "/" + table2);
            manifest.addAll(BackupTask.ssTableManifest(table2Path, backupRoot.resolve(table2Path.getFileName())));

            Map<Path, Path> manifestMap = new HashMap<>();
            for (ManifestEntry e : manifest) {
                manifestMap.put(e.localFile, e.objectKey);
            }

            if (testFileConfig.cassandraVersion == CassandraVersion.TWO_ZERO) {
                // table1 is un-compressed so should have written out a sha1 digest
                final Path localPath1 = table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)));

                Assert.assertEquals(manifestMap.get(localPath1),
                        backupRoot.resolve(String.format("%s/1-%s/%s-1-big-Data.db", table1, sha1Hash, testFileConfig.getSstablePrefix(keyspace, table1))));

                final Path localPath2 = table1Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table1)));
                final String checksum2 = BackupTask.calculateChecksum(localPath2);

                Assert.assertEquals(manifestMap.get(localPath2),
                        backupRoot.resolve(String.format("%s/3-%s/%s-3-big-Index.db", table1, checksum2, testFileConfig.getSstablePrefix(keyspace, table1))));

                final Path localPath3 = table2Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table2)));
                final String checksum3 = BackupTask.calculateChecksum(localPath3);

                Assert.assertEquals(manifestMap.get(localPath3),
                        backupRoot.resolve(String.format("%s/1-%s/%s-1-big-Data.db", table2, checksum3, testFileConfig.getSstablePrefix(keyspace, table2))));

                Assert.assertNull(manifestMap.get(table2Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table2)))));
            } else {
                Assert.assertEquals(manifestMap.get(table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)))),
                        backupRoot.resolve(String.format("%s/1-1000000000/%s-1-big-Data.db", table1, testFileConfig.getSstablePrefix(keyspace, table1))));

                // Cassandra doesn't create CRC32 file for 2.0.x
                Assert.assertEquals(manifestMap.get(table1Path.resolve(String.format("%s-2-big-Digest.crc32", testFileConfig.getSstablePrefix(keyspace, table1)))),
                        backupRoot.resolve(String.format("%s/2-1000000000/%s-2-big-Digest.crc32", table1, testFileConfig.getSstablePrefix(keyspace, table1))));

                Assert.assertEquals(manifestMap.get(table1Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table1)))),
                        backupRoot.resolve(String.format("%s/3-1000000000/%s-3-big-Index.db", table1, testFileConfig.getSstablePrefix(keyspace, table1))));

                Assert.assertEquals(manifestMap.get(table2Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table2)))),
                        backupRoot.resolve(String.format("%s/1-1000000000/%s-1-big-Data.db", table2, testFileConfig.getSstablePrefix(keyspace, table2))));

                Assert.assertEquals(manifestMap.get(table2Path.resolve(String.format("%s-2-big-Digest.crc32", testFileConfig.getSstablePrefix(keyspace, table2)))),
                        backupRoot.resolve(String.format("%s/2-1000000000/%s-2-big-Digest.crc32", table2, testFileConfig.getSstablePrefix(keyspace, table2))));

                Assert.assertNull(manifestMap.get(table2Path.resolve(String.format("%s-3-big-Index.db", testFileConfig.getSstablePrefix(keyspace, table2)))));
            }

            Assert.assertNull(manifestMap.get(table1Path.resolve("manifest.json")));
            Assert.assertNull(manifestMap.get(table1Path.resolve("backups")));
            Assert.assertNull(manifestMap.get(table1Path.resolve("snapshots")));
        }
    }




    @AfterClass(alwaysRun = true)
    public void cleanUp () throws IOException {
        testHelperService.deleteTempDirectories(tempDirs);
    }
}
