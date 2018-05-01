package com.instaclustr.backup.tasks;

import com.google.common.collect.ImmutableList;
import com.instaclustr.backup.jmx.CassandraVersion;
import com.instaclustr.backup.service.TestHelperService;
import com.instaclustr.backup.task.BackupTask;
import com.instaclustr.backup.task.ManifestEntry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class BackupTaskTest {
    private final String sha1Hash = "3a1bd6900872256303b1ed036881cd35f5b670ce";

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

    private void createSSTable(final Path folder, final String keyspace, final String table, final int sequence, final TestFileConfig testFileConfig, final boolean isCompressed) throws IOException {
        final Path ssTablePath = folder.resolve(keyspace).resolve(table);
        Files.createDirectories(ssTablePath);

        if (testFileConfig.createDigest(isCompressed)) {
            final Path digest = ssTablePath.resolve(String.format("%s-%s-big-Digest.crc32", testFileConfig.getSstablePrefix(keyspace, table), sequence));
            try (final Writer writer = Files.newBufferedWriter(digest)) {
                writer.write(testFileConfig.getChecksum(keyspace, table));
            }
        }

        final byte[] testData = UUID.randomUUID().toString().getBytes();
        for (String name : SSTABLE_FILES) {
            final Path path = ssTablePath.resolve(String.format("%s-%s-big-%s", testFileConfig.getSstablePrefix(keyspace, table), sequence, name));
            Files.createFile(path);
            Files.write(path, testData);
        }
    }

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        for (TestFileConfig testFileConfig : versionsToTest) {
            tempDirs.put(testFileConfig.cassandraVersion.name(), null);
        }

        testHelperService.setupTempDirectories(tempDirs);

        for (TestFileConfig testFileConfig : versionsToTest) {
            final Path root = tempDirs.get(testFileConfig.cassandraVersion.name());
            final Path path = root.resolve("data/");
            final String keyspace = "keyspace1";
            final String table1 = "table1";
            createSSTable(path, keyspace, table1, 1, testFileConfig, false);
            createSSTable(path, keyspace, table1, 2, testFileConfig, true);
            createSSTable(path, keyspace, table1, 3, testFileConfig, true);

            final String table2 = "table2";
            createSSTable(path, keyspace, table2, 1, testFileConfig, true);
            createSSTable(path, keyspace, table2, 2, testFileConfig, true);
        }
    }

    @Test
    public void testCalculateDigest() throws Exception {
        for (TestFileConfig testFileConfig : versionsToTest) {
            final String keyspace = "keyspace1";
            final String table1 = "table1";
            final Path table1Path = tempDirs.get(testFileConfig.cassandraVersion.name()).resolve("data/" + keyspace + "/" + table1);
            final Path path = table1Path.resolve(String.format("%s-1-big-Data.db", testFileConfig.getSstablePrefix(keyspace, table1)));
            final String checksum = BackupTask.calculateChecksum(path);

            Assert.assertNotNull(checksum);
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
