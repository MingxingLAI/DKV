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

package org.apache.dkv.storage.flush;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.dkv.storage.disk.DiskStorage;
import org.apache.dkv.storage.iterator.MemStoreIterator;
import org.apache.dkv.storage.memory.MemStore;
import org.apache.dkv.storage.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DefaultFlusherTest {

    private final TemporaryFolder folder = new TemporaryFolder();
    
    private DiskStorage diskStorage;

    private final MemStore memStore = mock(MemStore.class);
    
    @Before
    public void setUp() {
        try {
            folder.create();
            diskStorage = new DiskStorage(folder.getRoot().getAbsolutePath(), 10);
            when(memStore.getKvMap()).thenReturn(TestUtil.createKeyValuePairMap(Arrays.asList("1", "3", "5")));
            when(memStore.getSnapshot()).thenReturn(TestUtil.createKeyValuePairMap(Arrays.asList("2", "4", "6")));
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }
    
    @Test
    public void testFlush() throws IOException {
        diskStorage.open();
        
        assertThat(diskStorage.getMaxTableId(), equalTo(-1));
        assertThat(diskStorage.getTables(), equalTo(Collections.emptyList()));
        String fileName = folder.getRoot().getAbsolutePath() + File.separator + "SSTable00.sst";
        assertFalse(new File(fileName).exists());

        // first flush
        Flusher flusher = new DefaultFlusher(diskStorage);
        MemStoreIterator memStoreIterator = new MemStoreIterator(memStore);
        flusher.flush(memStoreIterator);
        assertThat(diskStorage.getMaxTableId(), equalTo(0));
        assertTrue(new File(fileName).exists());
        assertThat(diskStorage.getTables().size(), equalTo(1));
        
        // second flush
        memStoreIterator = new MemStoreIterator(memStore);
        flusher.flush(memStoreIterator);
        assertThat(diskStorage.getMaxTableId(), equalTo(1));
        fileName = folder.getRoot().getAbsolutePath() + File.separator + "SSTable01.sst";
        assertTrue(new File(fileName).exists());
        assertThat(diskStorage.getTables().size(), equalTo(2));
    }
}
