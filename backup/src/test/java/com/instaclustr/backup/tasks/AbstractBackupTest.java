package com.instaclustr.backup.tasks;

import com.google.cloud.storage.StorageOptions;
import com.instaclustr.backup.service.TestHelperService;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractBackupTest {
    final TestHelperService testHelperService = new TestHelperService();
    final String restoreFromNodeId = "cassandra-0";
    String sourceBucket = Optional.of(System.getenv("TEST_BUCKET")).orElse("cassandra-k8s-backuptest");


    Map<String, Path> tempDirs = new HashMap<String, Path>() {{
        this.put("test", null);
        this.put("data/system/available_ranges-c539fcabd65a31d18133d25605643ee3/", null);
    }};

    @BeforeClass(alwaysRun=true)
    public void setup() throws IOException, URISyntaxException {
        testHelperService.setupTempDirectories(tempDirs);
    }

}
