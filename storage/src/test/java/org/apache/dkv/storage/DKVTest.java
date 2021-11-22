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

package org.apache.dkv.storage;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bean.KeyValuePair.OperationType;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.config.Config;
import org.apache.dkv.storage.iterator.Iterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * DKV unit test
 */
public class DKVTest {

    private final TemporaryFolder folder = new TemporaryFolder();
    
    private DKV db;

    @Before
    public void setUp() throws IOException {
        folder.create();
        Config config = Config.builder().dataDir(folder.getRoot().getAbsolutePath()).maxMemstoreSize(2 * 1024 * 1024).flushMaxRetries(1).maxDiskFiles(10).maxThreadPoolSize(5).build();
        db = DKV.create(config).open();
    }
    
    @Test
    public void testPutAndGet() throws Exception {
        final byte[] bytesA = Bytes.toBytes("A");
        db.put(bytesA, bytesA);
        assertThat(db.get(bytesA).getValue(), equalTo(bytesA));

        db.delete(bytesA);
        Assert.assertNull(db.get(bytesA));

        final byte[] bytesB = Bytes.toBytes("B");
        db.put(bytesA, bytesB);
        assertThat(db.get(bytesA).getValue(), equalTo(bytesB));
        
        db.put(bytesB, bytesA);
        assertThat(db.get(bytesB).getValue(), equalTo(bytesA));
        
        db.put(bytesB, bytesB);
        assertThat(db.get(bytesB).getValue(), equalTo(bytesB));
        
        final byte[] bytesC = Bytes.toBytes("C");
        db.put(bytesC, bytesC);
        assertThat(db.get(bytesC).getValue(), equalTo(bytesC));
        
        db.delete(bytesB);
        Assert.assertNull(db.get(bytesB));
    }
    
    @Test
    public void testConcurrentPut() throws Exception {
        int totalElements = 100;
        int threadSize = 5;
        WriteDatabaseThread[] writers = new WriteDatabaseThread[threadSize];

        for (int i = 0; i < threadSize; i++) {
            int elementsPerThread = totalElements / threadSize;
            writers[i] = new WriteDatabaseThread(db, i * elementsPerThread, (i + 1) * elementsPerThread);
            writers[i].start();
        }
        for (int i = 0; i < threadSize; i++) {
            writers[i].join();
        }
        Iterator<KeyValuePair> iterator = db.scan();
        int index = 0;
        while (iterator.hasNext()) {
            KeyValuePair actual = iterator.next();
            KeyValuePair currentKeyValuePair = KeyValuePair.createPut(Bytes.toBytes(index), Bytes.toBytes(index), 0L);
            assertThat(actual.getKey(), equalTo(currentKeyValuePair.getKey()));
            assertThat(actual.getValue(), equalTo(currentKeyValuePair.getValue()));
            assertThat(actual.getOperationType(), equalTo(OperationType.Put));
            long sequenceId = actual.getSequenceId();
            assertTrue(sequenceId > 0);
            index++;
        }
        assertThat(index, equalTo(totalElements));
    }
    
    @Test
    public void testScan() throws IOException {
        int totalElements = 1000;
        List<Integer> data = IntStream.range(1, totalElements + 1).boxed().collect(Collectors.toList());
        Collections.shuffle(data);
        data.forEach(each -> {
            try {
                db.put(Bytes.toBytes(each), Bytes.toBytes(each));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        
        Iterator<KeyValuePair> iterator = db.scan();
        assertThat(countElements(iterator), equalTo(totalElements));
        
        iterator = db.scan(Bytes.toBytes(51), Bytes.EMPTY_BYTES);
        assertThat(countElements(iterator), equalTo(totalElements - 51 + 1));
        
        iterator = db.scan(Bytes.EMPTY_BYTES, Bytes.toBytes(33));
        assertThat(countElements(iterator), equalTo(32));
        
        iterator = db.scan(Bytes.toBytes(22), Bytes.toBytes(32));
        assertThat(countElements(iterator), equalTo(10));
    }
    
    private int countElements(final Iterator<KeyValuePair> iterator) throws IOException {
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            iterator.next();
        }
        return count;
    }
    
    @After
    public void tearDown() throws IOException {
        db.close();
        folder.delete();
    }
    
    @AllArgsConstructor
    private static class WriteDatabaseThread extends Thread {

        private final DKV db;

        private final int start;

        private final int stop;

        @Override
        public void run() {
            for (int i = start; i < stop; i++) {
                put(i);
            }
        }

        private void put(final int index) {
            int retries = 0;
            while (retries < 50) {
                try {
                    db.put(Bytes.toBytes(index), Bytes.toBytes(index));
                    break;
                } catch (IOException e) {
                    // MemStore maybe full, so let's retry.
                    retries++;
                    try {
                        Thread.sleep(100 * retries);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    } 
}
