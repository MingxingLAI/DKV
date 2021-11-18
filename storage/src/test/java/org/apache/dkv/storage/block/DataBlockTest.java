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

package org.apache.dkv.storage.block;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.IOException;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bean.KeyValuePair.OperationType;
import org.apache.dkv.storage.bloom.BloomFilter;
import org.apache.dkv.storage.bytes.Bytes;
import org.junit.Test;

public class DataBlockTest {

    @Test
    public void testDataBlock() throws IOException {
        DataBlock dataBlock = new DataBlock();
        for (int i = 0; i < 100; i++) {
            byte[] bytes = Bytes.toBytes(i);
            dataBlock.append(KeyValuePair.create(bytes, bytes, OperationType.Put, 1L));
        }
        assertThat(dataBlock.getLastKv(), equalTo(KeyValuePair.create(Bytes.toBytes(99), Bytes.toBytes(99), OperationType.Put, 1L)));
        byte[] buffer = dataBlock.serialize();
        DataBlock dataBlockFromByte = DataBlock.parseFrom(buffer, 0, buffer.length);
        
        byte[][] bytes = new byte[dataBlockFromByte.getKeyValueCount()][];
        for (int i = 0; i < dataBlockFromByte.getKeyValueCount(); i++) {
            bytes[i] = dataBlockFromByte.getKeyValuePairs().get(i).getKey();
        }
        BloomFilter bloom = new BloomFilter(DataBlock.BLOOM_FILTER_HASH_COUNT, DataBlock.BLOOM_FILTER_BITS_PER_KEY);
        assertThat(bloom.generate(bytes), equalTo(dataBlockFromByte.getBloomFilter()));
    }
}
