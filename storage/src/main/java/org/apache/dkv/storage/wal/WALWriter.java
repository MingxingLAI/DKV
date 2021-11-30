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

package org.apache.dkv.storage.wal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.dkv.storage.config.Config;

/**
 * write ahead log
 */
public final class WALWriter {

    private static final String WAL_SUFFIX = ".wal";

    private static final Pattern DATA_FILE_RE = Pattern.compile("dkv([0-9]+)\\.wal");
    
    private static final int MAX_BLOCK_SIZE = 32768;

    // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
    @Getter
    private static final int HEADER_SIZE = 4 + 2 + 1;
    
    private FileOutputStream out;

    private int blockOffset;
    
    private final String dataDir;

    private final AtomicInteger maxFileId;
    
    private final WriteOptions writeOptions;
    
    public WALWriter(final Config config) throws IOException {
        this.dataDir = config.getDataDir();
        this.writeOptions = config.getWriteOptions();
        this.maxFileId = new AtomicInteger(getMaxTableId());
        
        File f = new File(getNexTableFileName());
        assert f.createNewFile();
        out = new FileOutputStream(f);
        blockOffset = 0;
    }

    /**
     * get max table file id.
     * @return table file id
     */
    public synchronized int getMaxTableId() {
        File[] files = getAllTableFiles();
        int maxFileId = -1;
        for (File f : files) {
            Matcher matcher = DATA_FILE_RE.matcher(f.getName());
            if (matcher.matches()) {
                maxFileId = Math.max(Integer.parseInt(matcher.group(1)), maxFileId);
            }
        }
        return maxFileId;
    }

    public synchronized String getNexTableFileName() {
        return new File(this.dataDir, String.format("dkv%02d.wal", nexTableId())).toString();
    }
    
    /**
     * get next table file id.
     * @return table file id
     */
    public synchronized int nexTableId() {
        return maxFileId.incrementAndGet();
    }

    private File[] getAllTableFiles() {
        File dir = new File(this.dataDir);
        return dir.listFiles(each -> DATA_FILE_RE.matcher(each.getName()).matches());
    }
    
    /**
     * append a log record into wal file.
     * @param record log record.
     * @throws IOException IO Exception.
     */
    public void addRecord(final byte[] record) throws IOException {
        int left = record.length;
        int pos = 0;
        boolean begin = true;
        do {
            int leftover = MAX_BLOCK_SIZE - blockOffset;
            assert leftover >= 0;
            if (leftover < HEADER_SIZE) {
                switchWALFile(leftover);
            }
            // Invariant: we never leave < kHeaderSize bytes in a block.
            int avail = MAX_BLOCK_SIZE - blockOffset - HEADER_SIZE;
            assert avail >= 0;
            int fragmentLength = Math.min(left, avail);
            RecordType type = getRecordType(begin, left == fragmentLength);
            writePhysicalRecord(type, record, pos, fragmentLength);
            left -= fragmentLength;
            begin = false;
        } while (left > 0);
    }
    
    private RecordType getRecordType(final boolean begin, final boolean end) {
        if (begin && end) {
            return RecordType.FullType;
        } else if (begin) {
            return RecordType.FirstType;
        } else if (end) {
            return RecordType.LastType;
        } else {
            return RecordType.MiddleType;
        }
    }
    
    private void writePhysicalRecord(final RecordType type, final byte[] record, final int offset, final int length)
            throws IOException {
        // must fit in two bytes
        assert length <= 0xffff;
        assert blockOffset + HEADER_SIZE + length <= MAX_BLOCK_SIZE;
        
        // TODO cal checksum
        RecordHeader header = new RecordHeader(0, type, length);

        // write the header and payload
        out.write(header.serialize());
        out.write(record, offset, length);
        blockOffset += HEADER_SIZE + length;
    }
    
    private void switchWALFile(final int leftover) throws IOException {
        // switch to a new block
        if (leftover > 0) {
            out.write(new byte[HEADER_SIZE], 0, leftover);
        }
        blockOffset = 0;
    }

    /**
     * switch to next wal file.
     * @throws IOException IO Exception.
     */
    public synchronized void switchNextWALFile() throws IOException {
        out.close();
        File f = new File(getNexTableFileName());
        assert f.createNewFile();
        out = new FileOutputStream(f);
    }

    /**
     * sync wal record into disk
     * @throws IOException IO Exception
     */
    public void sync() throws IOException {
        if (WriteOptions.sync == writeOptions) {
            out.flush();
            out.getFD().sync();
        } else if (WriteOptions.flush == writeOptions) {
            out.flush();
        }
        // do nothing if write option is noop
    }
}
