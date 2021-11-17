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

package org.apache.dkv.storage.bean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import java.io.IOException;
import org.apache.dkv.storage.bean.KeyValuePair.OperationType;
import org.apache.dkv.storage.bytes.Bytes;
import org.junit.Test;

public class KeyValuePairTest {
    
    @Test
    public void testCompare() {
        KeyValuePair kv = KeyValuePair.createPut(Bytes.toBytes(100), Bytes.toBytes(1000), 0L);
        assertThat(kv, notNullValue());
        assertThat(kv, not(new Object()));
        assertThat(KeyValuePair.createPut(Bytes.toBytes(100), Bytes.toBytes(1000), 0L), equalTo(kv));
        assertThat(KeyValuePair.createPut(Bytes.toBytes(100L), Bytes.toBytes(1000), 0L), not(equalTo(kv)));
        assertThat(KeyValuePair.createPut(Bytes.toBytes(100), Bytes.toBytes(1000L), 0L), equalTo(kv));
    }

    @Test
    public void testCreateKeyValuePair() {
        KeyValuePair addedKeyValuePair = KeyValuePair.create(Bytes.toBytes("name"), Bytes.toBytes("dvk"), OperationType.Put, 10);
        assertThat(KeyValuePair.createPut(Bytes.toBytes("name"), Bytes.toBytes("dkv"), 10), equalTo(addedKeyValuePair));

        KeyValuePair deletedKeyValuePair = KeyValuePair.create(Bytes.toBytes("name"), Bytes.EMPTY_BYTES, OperationType.Delete, 11);
        assertThat(KeyValuePair.createDelete(Bytes.toBytes("name"), 11), equalTo(deletedKeyValuePair));
    }
    
    @Test
    public void testToBytes() throws IOException {
        KeyValuePair keyValuePair = KeyValuePair.create(Bytes.toBytes("name"), Bytes.toBytes("dvk"), OperationType.Put, 10);
        byte[] result = keyValuePair.toBytes();
        KeyValuePair actual = KeyValuePair.parseFrom(result);
        assertThat(actual.getKey(), equalTo(keyValuePair.getKey()));
        assertThat(actual.getValue(), equalTo(keyValuePair.getValue()));
        assertThat(actual.getOperationType(), equalTo(keyValuePair.getOperationType()));
        assertThat(actual.getSequenceId(), equalTo(keyValuePair.getSequenceId()));
    }
    
    @Test
    public void testGetSerializeSize() {
        KeyValuePair keyValuePair = KeyValuePair.create(Bytes.toBytes(1), Bytes.toBytes(1), OperationType.Put, 1);
        assertThat(keyValuePair.getSerializeSize(), equalTo(4 + 4 + 8 + 1 + 4 + 4));
    }
}
