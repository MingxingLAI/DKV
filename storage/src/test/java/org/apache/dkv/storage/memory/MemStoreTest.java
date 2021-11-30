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

package org.apache.dkv.storage.memory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bean.KeyValuePair.OperationType;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.config.Config;
import org.apache.dkv.storage.flush.Flusher;
import org.apache.dkv.storage.iterator.SeekIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MemStoreTest {

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
    public void testMemStoreAdd() throws IOException {
        MemStore memStore = createMemStore();
        assertThat(memStore.getDataSize().get(), equalTo(100L));
        assertThat(memStore.getSnapshot(), equalTo(null));
    }
    
    @Test(expected = IOException.class)
    public void testUpToCapacityLimit() throws IOException {
        MemStore memStore = createMemStore();
        memStore.add(KeyValuePair.create(Bytes.toBytes(5), Bytes.toBytes(5), OperationType.Put, 5));
        memStore.add(KeyValuePair.create(Bytes.toBytes(6), Bytes.toBytes(6), OperationType.Put, 6));
    }

    @Test
    public void testMemStoreIterator() throws IOException {
        MemStore memStore = createMemStore();
        SeekIterator<KeyValuePair> iterator = memStore.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count, equalTo(4));
    }
    
    private MemStore createMemStore() throws IOException {
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.submit(any(Runnable.class))).thenReturn(null);
        Flusher flusher = mock(Flusher.class);
        MemStore memStore = new MemStore(Config.builder().dataDir(folder.getRoot().getAbsolutePath()).maxMemstoreSize(100).build(), flusher, executorService);
        memStore.add(KeyValuePair.create(Bytes.toBytes(1), Bytes.toBytes(1), OperationType.Put, 1));
        assertThat(memStore.getDataSize().get(), equalTo(25L));
        memStore.add(KeyValuePair.create(Bytes.toBytes(2), Bytes.toBytes(2), OperationType.Put, 2));
        memStore.add(KeyValuePair.create(Bytes.toBytes(3), Bytes.toBytes(3), OperationType.Put, 3));
        memStore.add(KeyValuePair.create(Bytes.toBytes(4), Bytes.toBytes(4), OperationType.Put, 4));
        return memStore;
    }
    
    @After
    public void tearDown() {
        folder.delete();
    }
}
