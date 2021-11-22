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
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.iterator.SeekIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableTest {

    private final TemporaryFolder folder = new TemporaryFolder();

    private String fileName;
    
    private SSTableBuilder builder;
    
    @Before
    public void setUp() {
        try {
            folder.create();
            fileName = folder.getRoot().getAbsolutePath() + File.separator + "SSTable01.sst";
            builder = new SSTableBuilder(fileName);
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    @Test
    public void testSSTableBuilder() throws IOException {
        SSTableBuilder builder = initSSTable();
        File f = new File(fileName);
        assertThat(f.length(), equalTo(builder.getFileSize()));
        
        RandomAccessFile in = new RandomAccessFile(f, "r");
        in.seek(f.length() - TailerBlock.TAILER_SIZE);
        byte[] buffer = new byte[TailerBlock.TAILER_SIZE];
        assertThat(in.read(buffer), equalTo(TailerBlock.TAILER_SIZE));
        
        TailerBlock tailerBlock = TailerBlock.parseFrom(buffer, 0);
        assertTailerBlock(tailerBlock, builder.getTailerBlock());
    }
    
    @Test
    public void testSSTable() throws IOException {
        SSTableBuilder builder = initSSTable();
        SSTable table = new SSTable(fileName);
        assertTailerBlock(table.getTailerBlock(), builder.getTailerBlock());
        SeekIterator<KeyValuePair> iterator = table.iterator();
        int count = 0;
        KeyValuePair lastKeyValuePair = null;
        while (iterator.hasNext()) {
            lastKeyValuePair = iterator.next();
            count++;
        }
        // how many elements.
        assertThat(count, equalTo(10000));
        assertThat(table.getTailerBlock().getBlockCount(), equalTo(table.getIndexBlocks().size()));
        assertThat(table.getIndexBlocks().last().getLastKv(), equalTo(lastKeyValuePair));
    }
    
    @Test
    public void testSSTableSeek() throws IOException {
        for (int i = 0; i < 100; i++) {
            KeyValuePair lastKv = KeyValuePair.createPut(Bytes.toBytes(i), Bytes.toBytes(i), 1L);
            builder.append(lastKv);
        }
        builder.appendIndex();
        builder.appendTailer();

        SSTable table = new SSTable(fileName);
        SeekIterator<KeyValuePair> iterator = table.iterator();
        iterator.seekTo(KeyValuePair.createPut(Bytes.toBytes(49), Bytes.toBytes(49), 1L));
        // 40
        iterator.next();
        // 50
        KeyValuePair actual = iterator.next();
        // contain 49 and 50, iterator to 99, total 51 elements.
        int count = 2;
        while (iterator.hasNext()) {
            iterator.next();
            count += 1;
        }
        assertThat(count, equalTo(51));
        assertThat(actual, equalTo(KeyValuePair.createPut(Bytes.toBytes(50), Bytes.toBytes(50), 1L)));
    }
    
    private void assertTailerBlock(final TailerBlock actual, final TailerBlock expected) {
        assertThat(actual.getMagicNumber(), equalTo(expected.getMagicNumber()));
        assertThat(actual.getFileSize(), equalTo(expected.getFileSize()));
        assertThat(actual.getBlockCount(), equalTo(expected.getBlockCount()));
        assertThat(actual.getIndexBlockOffset(), equalTo(expected.getIndexBlockOffset()));
        assertThat(actual.getIndexBlockSize(), equalTo(expected.getIndexBlockSize()));
    }
    
    private SSTableBuilder initSSTable() throws IOException {
        KeyValuePair lastKv;
        for (int i = 0; i < 10000; i++) {
            lastKv = KeyValuePair.createPut(generateRandomBytes(), generateRandomBytes(), 1L);
            builder.append(lastKv);
        }
        builder.appendIndex();
        builder.appendTailer();
        return builder;
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
    public void tearDown() throws IOException {
        builder.close();
        folder.delete();
    }
}
