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

package org.apache.dkv.storage.bytes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;

public final class BytesTest {

    @Test
    public void testToByte() {
        assertThat(Bytes.toBytes((byte) 'a'), equalTo(new byte[]{'a'}));
    }

    @Test
    public void testToHex() {
        byte[] bytes = Bytes.toBytes(567890);
        assertThat(Bytes.toHex(bytes, 0, bytes.length), equalTo("\\x00\\x08\\xAAR"));
    }

    @Test
    public void testToInt() {
        assertThat(Bytes.toInt(Bytes.toBytes(123456)), equalTo(123456));
        assertThat(Bytes.toInt(Bytes.toBytes(0)), equalTo(0));
        assertThat(Bytes.toInt(Bytes.toBytes(1)), equalTo(1));
        assertThat(Bytes.toInt(Bytes.toBytes(-1)), equalTo(-1));
        assertThat(Bytes.toInt(Bytes.toBytes(Integer.MIN_VALUE)), equalTo(Integer.MIN_VALUE));
        assertThat(Bytes.toInt(Bytes.toBytes(Integer.MAX_VALUE)), equalTo(Integer.MAX_VALUE));
    }

    @Test
    public void testToLong() {
        assertThat(Bytes.toLong(Bytes.toBytes(123456L)), equalTo(123456L));
        assertThat(Bytes.toLong(Bytes.toBytes(0L)), equalTo(0L));
        assertThat(Bytes.toLong(Bytes.toBytes(1L)), equalTo(1L));
        assertThat(Bytes.toLong(Bytes.toBytes(-1L)), equalTo(-1L));
        assertThat(Bytes.toLong(Bytes.toBytes(Long.MIN_VALUE)), equalTo(Long.MIN_VALUE));
        assertThat(Bytes.toLong(Bytes.toBytes(Long.MAX_VALUE)), equalTo(Long.MAX_VALUE));
    }

    @Test
    public void testBytesCompare() {
        assertThat(Bytes.compare(null, null), equalTo(0));
        assertThat(Bytes.compare(new byte[]{0x00}, new byte[0]), equalTo(1));
        assertThat(Bytes.compare(new byte[]{0x00}, new byte[]{0x00}), equalTo(0));
        assertThat(Bytes.compare(new byte[]{0x00}, null), equalTo(1));
        assertThat(Bytes.compare(new byte[]{0x00}, new byte[]{0x01}), equalTo(-1));
    }
    
    @Test
    public void testTwoBytesToInt() {
        int max = 1 << 15;
        for (int i = 0; i < max; i++) {
            assertThat(Bytes.twoBytesToInt(Bytes.toTwoBytes(i)), equalTo(i));
        }
    }
}
