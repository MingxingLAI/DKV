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

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.iterator.SeekIterator;

@AllArgsConstructor
@Getter
public final class DataBlockMeta implements Comparable<DataBlockMeta> {

    private static final int OFFSET_SIZE = 8;
    
    private static final int SIZE_SIZE = 8;
    
    private static final int BF_LEN_SIZE = 4;
    
    private final KeyValuePair lastKv;
    
    private final long blockOffset;
    
    private final long blockSize;
    
    private final byte[] bloomFilter;

    /**
     * Only used for {@link SeekIterator} to seek a target block meta. we only care about the lastKV, so
     * the other fields can be anything.
     *
     * @param lastKV the last key value to construct the dummy block meta.
     * @return the dummy block meta.
     */
    private static DataBlockMeta createSeekDummy(final KeyValuePair lastKV) {
        return new DataBlockMeta(lastKV, 0L, 0L, Bytes.EMPTY_BYTES);
    }

    /**
     *  get serialize size.
     * @return size.
     */
    public int getSerializeSize() {
        return lastKv.getSerializeSize() + OFFSET_SIZE + SIZE_SIZE + BF_LEN_SIZE + bloomFilter.length;
    }

    /**
     * convert block to bytes.
     * @return byte array.
     * @throws IOException
     */
    public byte[] toBytes() throws IOException {
        byte[] bytes = new byte[getSerializeSize()];
        int pos = 0;

        // encode last kv
        byte[] kvBytes = lastKv.toBytes();
        System.arraycopy(kvBytes, 0, bytes, pos, kvBytes.length);
        pos += kvBytes.length;

        // encode blockOffset
        byte[] offsetBytes = Bytes.toBytes(blockOffset);
        System.arraycopy(offsetBytes, 0, bytes, pos, offsetBytes.length);
        pos += offsetBytes.length;

        // encode blockSize
        byte[] sizeBytes = Bytes.toBytes(blockSize);
        System.arraycopy(sizeBytes, 0, bytes, pos, sizeBytes.length);
        pos += sizeBytes.length;

        // encode length of bloom filter
        byte[] bfLenBytes = Bytes.toBytes(bloomFilter.length);
        System.arraycopy(bfLenBytes, 0, bytes, pos, bfLenBytes.length);
        pos += bfLenBytes.length;
        
        // encode bytes of bloom filter
        System.arraycopy(bloomFilter, 0, bytes, pos, bloomFilter.length);
        pos += bloomFilter.length;

        if (pos != bytes.length) {
            throw new IOException("pos(" + pos + ") should be equal to length of bytes (" + bytes.length + ")");
        }
        return bytes;
    }

    public static DataBlockMeta parseFrom(final byte[] buf, final int offset) throws IOException {
        int pos = offset;
        // Decode last key value.
        KeyValuePair lastKV = KeyValuePair.parseFrom(buf, offset);
        pos += lastKV.getSerializeSize();

        // Decode block blockOffset
        final long blockOffset = Bytes.toLong(Bytes.slice(buf, pos, OFFSET_SIZE));
        pos += OFFSET_SIZE;

        // Decode block blockSize
        final long blockSize = Bytes.toLong(Bytes.slice(buf, pos, SIZE_SIZE));
        pos += SIZE_SIZE;

        // Decode blockSize of block bloom filter
        int bloomFilterSize = Bytes.toInt(Bytes.slice(buf, pos, BF_LEN_SIZE));
        pos += BF_LEN_SIZE;

        // Decode bytes of block bloom filter
        byte[] bloomFilter = Bytes.slice(buf, pos, bloomFilterSize);
        pos += bloomFilterSize;

        assert pos <= buf.length;
        return new DataBlockMeta(lastKV, blockOffset, blockSize, bloomFilter);
    }
    
    @Override
    public int compareTo(final DataBlockMeta o) {
        return this.getLastKv().compareTo(o.getLastKv());
    }
}
