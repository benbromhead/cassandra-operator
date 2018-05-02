package com.instaclustr.backup;

import com.instaclustr.backup.task.RestoreTask;
import com.instaclustr.backup.util.CloudDownloadUploadFactory;
import com.instaclustr.backup.util.GlobalLock;


public class RestoreTaskLauncher {
        public static void run(final RestoreArguments restoreArguments) throws Exception {
            GlobalLock globalLock = new GlobalLock(restoreArguments.sharedContainerPath.toString());
            new RestoreTask(CloudDownloadUploadFactory.getDownloader(restoreArguments),
                restoreArguments.cassandraDirectory,
                restoreArguments.cassandraConfigDirectory,
                restoreArguments.sharedContainerPath,
//                Optional.empty(),
                    globalLock,
                    restoreArguments,
                    restoreArguments.keyspaceTables).call();


    }
}
