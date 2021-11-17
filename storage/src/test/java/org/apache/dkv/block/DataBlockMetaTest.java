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

package org.apache.dkv.block;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.IOException;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.block.DataBlockMeta;
import org.apache.dkv.storage.bytes.Bytes;
import org.junit.Test;

public class DataBlockMetaTest {
    
    @Test
    public void testDataBlockMeta() throws IOException {
        KeyValuePair lastKv = KeyValuePair.createPut(Bytes.toBytes("abc"), Bytes.toBytes("abc"), 1L);
        long offset = 1024;
        long size = 1024;
        byte[] bloomFilter = Bytes.toBytes("bloomFilter");
        DataBlockMeta meta = new DataBlockMeta(lastKv, offset, size, bloomFilter);
        byte[] buffer = meta.toBytes();
        DataBlockMeta metaFromBytes = DataBlockMeta.parseFrom(buffer, 0);
        assertThat(metaFromBytes.getLastKv(), equalTo(lastKv));
        assertThat(metaFromBytes.getBlockOffset(), equalTo(offset));
        assertThat(metaFromBytes.getBlockSize(), equalTo(size));
        assertThat(metaFromBytes.getBloomFilter(), equalTo(bloomFilter));
    }
}
