/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dkv.storage.compact;

import java.io.File;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.config.Config;
import org.apache.dkv.storage.disk.DiskStorage;
import org.apache.dkv.storage.disk.SSTableBuilder;
import org.apache.dkv.storage.disk.SSTable;
import org.apache.dkv.storage.iterator.Iterator;
import org.apache.dkv.storage.util.FileUtil;

@Slf4j
public final class DefaultCompactor implements Compactor {
    
    private DiskStorage diskStorage;
    
    private volatile boolean running = true;
    
    public DefaultCompactor(final DiskStorage diskStorage) {
        this.diskStorage = diskStorage;
    }
    
    @Override
    public void compact() throws IOException {
        List<SSTable> filesToCompact = diskStorage.getTables();
        String fileName = diskStorage.getNexTableFileName();
        String tempFileName = fileName + DiskStorage.FILE_NAME_TMP_SUFFIX;

        try (SSTableBuilder builder = new SSTableBuilder(tempFileName)) {
            for (Iterator<KeyValuePair> it = diskStorage.iterator(filesToCompact); it.hasNext();) {
                builder.append(it.next());
            }
            builder.appendIndex();
            builder.appendTailer();
        }
        try {
            // step 1 create new SSTable file
            FileUtil.rename(tempFileName, fileName);
            // step 2 archive history SSTable file
            archiveHistoryFiles(filesToCompact);
            // step 3 add newest SSTable file into DiskStorage
            diskStorage.addTable(fileName);
        } finally {
            File f = new File(tempFileName);
            if (f.exists()) {
                f.delete();
            }
        }
    }
    
    private void archiveHistoryFiles(final List<SSTable> diskFiles) throws IOException {
        for (SSTable table : diskFiles) {
            table.close();
            File f = new File(table.getFileName());
            File archiveFile = new File(table.getFileName() + DiskStorage.FILE_NAME_ARCHIVE_SUFFIX);
            if (!f.renameTo(archiveFile)) {
                log.error("Rename " + table.getFileName() + " to " + archiveFile.getName() + " failed.");
            }
        }
        diskStorage.removeObsoleteTables(diskFiles);
    }
    
    @Override
    public void run() {
        while (running) {
            try {
                boolean isCompacted = false;
                // perform compact if SSTable file is too many
                if (diskStorage.getTables().size() > Config.getDefault().getMaxDiskFiles()) {
                    compact();
                    isCompacted = true;
                }
                if (!isCompacted) {
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                // only record compact failure
                log.error("major compaction failed: {}", e);
            } catch (InterruptedException ex) {
                // stop running compact thread
                log.error("Interrupted Exception, stop running: {}", ex);
                break;
            }
        }
    }

    /**
     * stop compact operation.
     */
    public void stopRunning() {
        this.running = false;
    }
}
