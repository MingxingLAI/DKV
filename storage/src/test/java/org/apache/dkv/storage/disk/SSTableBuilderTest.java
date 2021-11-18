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

package org.apache.dkv.storage.disk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.block.TailerBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableBuilderTest {

    private final TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() {
        try {
            folder.create();
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    @Test
    public void testSSTableBuilder() throws IOException {
        String fileName = folder.getRoot().getAbsolutePath() + File.separator + "SSTable01.sst";
        long fileSize = initSSTable(fileName);
        File f = new File(fileName);
        assertThat(f.length(), equalTo(fileSize));
        
        RandomAccessFile in = new RandomAccessFile(f, "r");
        in.seek(f.length() - TailerBlock.TAILER_SIZE);
        byte[] buffer = new byte[TailerBlock.TAILER_SIZE];
        assertThat(in.read(buffer), equalTo(TailerBlock.TAILER_SIZE));
        
        TailerBlock tailerBlock = TailerBlock.parseFrom(buffer, 0);
        assertThat(tailerBlock.getFileSize(), equalTo(f.length()));
        assertThat(tailerBlock.getMagicNumber(), equalTo(TailerBlock.DISK_FILE_MAGIC));
    }
    
    private long initSSTable(final String fileName) throws IOException {
        long fileSize;
        try (SSTableBuilder builder = new SSTableBuilder(fileName)) {
            KeyValuePair lastKv = null;
            for (int i = 0; i < 10000; i++) {
                lastKv = KeyValuePair.createPut(generateRandomBytes(), generateRandomBytes(), 1L);
                builder.append(lastKv);
            }
            builder.appendIndex();
            builder.appendTailer();
            fileSize = builder.getFileSize();
            assertThat(lastKv, equalTo(builder.getCurrentDataBlock().getLastKv()));
        }
        return fileSize;
    }

    private byte[] generateRandomBytes() {
        int len = (ThreadLocalRandom.current().nextInt() % 1024 + 1024) % 1024;
        byte[] buffer = new byte[len];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) (ThreadLocalRandom.current().nextInt() & 0xFF);
        }
        return buffer;
    }
    
    @After
    public void tearDown() {
        folder.delete();
    }
}
