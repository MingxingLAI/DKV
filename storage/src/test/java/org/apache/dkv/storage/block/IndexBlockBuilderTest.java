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
import org.apache.dkv.storage.bytes.Bytes;
import org.junit.Test;

public class IndexBlockBuilderTest {

    @Test
    public void testDataBlockMetaBuilder() throws IOException {
        IndexBlockBuilder builder = new IndexBlockBuilder();
        builder.append(KeyValuePair.createPut(Bytes.toBytes(1), Bytes.toBytes(1), 1), 100, 100, Bytes.EMPTY_BYTES);
        builder.append(KeyValuePair.createPut(Bytes.toBytes(2), Bytes.toBytes(2), 2), 200, 101, Bytes.EMPTY_BYTES);
    
        byte[] result = builder.serialize();
        IndexBlock indexBlock1 = IndexBlock.parseFrom(result, 0);
        assertThat(indexBlock1.getBlockOffset(), equalTo(100L));
        assertThat(indexBlock1.getBlockSize(), equalTo(100L));
        assertThat(indexBlock1.getLastKv(), equalTo(KeyValuePair.createPut(Bytes.toBytes(1), Bytes.toBytes(1), 1)));
        IndexBlock indexBlock2 = IndexBlock.parseFrom(result, result.length / 2);
        assertThat(indexBlock2.getBlockOffset(), equalTo(200L));
        assertThat(indexBlock2.getBlockSize(), equalTo(101L));
        assertThat(indexBlock2.getLastKv(), equalTo(KeyValuePair.createPut(Bytes.toBytes(2), Bytes.toBytes(2), 2)));
    }
}
