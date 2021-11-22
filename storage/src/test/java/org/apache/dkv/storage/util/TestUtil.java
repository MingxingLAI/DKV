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

package org.apache.dkv.storage.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.disk.DiskStorage;
import org.apache.dkv.storage.flush.DefaultFlusher;
import org.apache.dkv.storage.flush.Flusher;
import org.apache.dkv.storage.iterator.MemStoreIterator;
import org.apache.dkv.storage.memory.MemStore;

/**
 * utility for unit test
 */
public class TestUtil {

    /**
     * generate skip list
     * @param data key list
     * @return skip list
     */
    public static ConcurrentSkipListMap<KeyValuePair, KeyValuePair> createKeyValuePairMap(final List<String> data) {
        Collection<KeyValuePair> keyValuePairs = data.stream().map(each -> KeyValuePair.createPut(Bytes.toBytes(each), Bytes.toBytes(each), 1)).collect(Collectors.toSet());
        ConcurrentSkipListMap<KeyValuePair, KeyValuePair> result = new ConcurrentSkipListMap<>();
        for (KeyValuePair keyValuePair: keyValuePairs) {
            result.put(keyValuePair, keyValuePair);
        }
        return result;
    }

    /**
     * create SSTable
     * @param diskStorage disk storage
     * @throws IOException IO Exception
     */
    public static void createSSTables(final DiskStorage diskStorage) throws IOException {
        Flusher flusher = new DefaultFlusher(diskStorage);
        MemStoreIterator memStoreIterator = createNewMemStore(Arrays.asList("1", "3", "5"), Arrays.asList("2", "4", "6"));
        flusher.flush(memStoreIterator);

        memStoreIterator = createNewMemStore(Arrays.asList("a", "c", "e"), Arrays.asList("b", "d", "f"));
        flusher.flush(memStoreIterator);
    }

    private static MemStoreIterator createNewMemStore(final List<String> data, final List<String> snapshot) throws IOException {
        final MemStore memStore = mock(MemStore.class);
        when(memStore.getKvMap()).thenReturn(TestUtil.createKeyValuePairMap(data));
        when(memStore.getSnapshot()).thenReturn(TestUtil.createKeyValuePairMap(snapshot));
        return new MemStoreIterator(memStore);
    }
}
