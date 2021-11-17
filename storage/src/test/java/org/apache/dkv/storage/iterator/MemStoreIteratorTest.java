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

package org.apache.dkv.storage.iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bean.KeyValuePair.OperationType;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.memory.MemStore;
import org.junit.Before;
import org.junit.Test;

public class MemStoreIteratorTest {

    private final MemStore memStore = mock(MemStore.class);
    
    @Before
    public void setUp() {
        when(memStore.getKvMap()).thenReturn(createKeyValuePairMap(Arrays.asList("1", "3", "5")));
        when(memStore.getSnapshot()).thenReturn(createKeyValuePairMap(Arrays.asList("2", "4", "6")));
    }
    
    @Test
    public void testMemStoreIterator() throws IOException {
        MemStoreIterator memStoreIterator = new MemStoreIterator(memStore);
        int count = 0;
        KeyValuePair previous = null;
        while (memStoreIterator.hasNext()) {
            count += 1;
            KeyValuePair current = memStoreIterator.next();
            if (null != previous) {
                assertThat(current, greaterThan(previous));
            }
            previous = current;
        }
        assertThat(count, equalTo(6));
    }

    @Test
    public void testSeekTo() {
        IteratorWrapper iteratorWrapper = new IteratorWrapper(memStore.getKvMap());
        iteratorWrapper.seekTo(KeyValuePair.create(Bytes.toBytes("2"), Bytes.toBytes("2"), OperationType.Put, 1));
        int count = 0;
        while (iteratorWrapper.hasNext()) {
            iteratorWrapper.next();
            count += 1;
        }
        assertThat(count, equalTo(2));
    }
    
    private ConcurrentSkipListMap<KeyValuePair, KeyValuePair> createKeyValuePairMap(final List<String> data) {
        Collection<KeyValuePair> keyValuePairs = data.stream().map(each -> KeyValuePair.create(Bytes.toBytes(each), Bytes.toBytes(each), OperationType.Put, 1)).collect(Collectors.toSet());
        ConcurrentSkipListMap<KeyValuePair, KeyValuePair> result = new ConcurrentSkipListMap<>();
        for (KeyValuePair keyValuePair: keyValuePairs) {
            result.put(keyValuePair, keyValuePair);
        }
        return result;
    }
}
