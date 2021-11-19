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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import lombok.Data;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bloom.BloomFilter;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.bytes.BytesBuilder;

/**
 * build and parse DataBlock.
 */
@Data
public final class DataBlock {

    public static final int KV_SIZE_LEN = 4;
    
    public static final int CHECKSUM_LEN = 4;

    // duplicate define
    public static final int BLOOM_FILTER_HASH_COUNT = 3;
    
    public static final int BLOOM_FILTER_BITS_PER_KEY = 10;

    private int totalSize;
    
    private List<KeyValuePair> keyValuePairs;
    
    private final BloomFilter bloomFilter;
    
    private Checksum crc32;
    
    private KeyValuePair lastKv;
    
    public DataBlock() {
        totalSize = 0;
        keyValuePairs = new ArrayList<>();
        bloomFilter = new BloomFilter(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_BITS_PER_KEY);
        crc32 = new CRC32();
    }
    
    public DataBlock(final int totalSize, final List<KeyValuePair> keyValuePairs) {
        this.totalSize = totalSize;
        this.keyValuePairs = keyValuePairs;
        bloomFilter = new BloomFilter(BLOOM_FILTER_HASH_COUNT, BLOOM_FILTER_BITS_PER_KEY);
    }

    /**
     * add Key Value into block
     * @param keyValuePair to append to data block.
     */
    public void append(final KeyValuePair keyValuePair) {
        // update key value buffer
        keyValuePairs.add(keyValuePair);
        lastKv = keyValuePair;
        
        // update checksum
        byte[] buf = keyValuePair.toBytes();
        crc32.update(buf, 0, buf.length);
        
        totalSize += keyValuePair.getSerializeSize();
    }

    /**
     * generate bloom filter.
     * @return byte array represent bloom filter.
     */
    public byte[] getBloomFilter() {
        byte[][] bytes = new byte[keyValuePairs.size()][];
        for (int i = 0; i < keyValuePairs.size(); i++) {
            bytes[i] = keyValuePairs.get(i).getKey();
        }
        return bloomFilter.generate(bytes);
    }

    /**
     * get checksum digest.
     * @return integer represent checksum digest.
     */
    public int getChecksum() {
        return (int) crc32.getValue();
    }

    /**
     * get block size.
     * @return integer represent block size.
     */
    public int getSize() {
        return KV_SIZE_LEN + totalSize + CHECKSUM_LEN;
    }

    /**
     * get the count of Key Value.
     * @return integer represent count of Key Value.
     */
    public int getKeyValueCount() {
        return keyValuePairs.size();
    }

    /**
     * is block has no key value pair.
     * @return true if there is no any key value pair.
     */
    public boolean isEmpty() {
        return keyValuePairs.size() == 0;
    }

    /**
     * serialize a data block.
     * @return byte array represent a data block
     */
    public byte[] serialize() {
        BytesBuilder builder = new BytesBuilder(getSize());

        // encode count
        byte[] countBytes = Bytes.toBytes(keyValuePairs.size());
        builder.append(countBytes);

        // Append all the key value
        for (KeyValuePair keyValuePair : keyValuePairs) {
            byte[] kv = keyValuePair.toBytes();
            builder.append(kv);
        }

        // Append checksum.
        byte[] checksum = Bytes.toBytes(this.getChecksum());
        builder.append(checksum);

        assert builder.getPos() == getSize();
        return builder.getBuffer();
    }

    /**
     * construct Data Block from bytes.
     * @param buffer byte array.
     * @param offset block offset.
     * @param size block size.
     * @return Data Block.
     * @throws IOException error.
     */
    public static DataBlock parseFrom(final byte[] buffer, final int offset, final int size) throws IOException {
        int pos = 0;
        // Parse kv getSerializeSize
        int count = Bytes.toInt(Bytes.slice(buffer, offset + pos, KV_SIZE_LEN));
        pos += KV_SIZE_LEN;

        // parse all key value
        List<KeyValuePair> result = new ArrayList<>(count);
        Checksum crc32 = new CRC32();
        for (int i = 0; i < count; i++) {
            KeyValuePair keyValuePair = KeyValuePair.parseFrom(buffer, offset + pos);
            result.add(keyValuePair);
            crc32.update(buffer, offset + pos, keyValuePair.getSerializeSize());
            pos += keyValuePair.getSerializeSize();
        }

        // parse checksum
        int checksum = Bytes.toInt(Bytes.slice(buffer, offset + pos, CHECKSUM_LEN));
        pos += CHECKSUM_LEN;

        assert checksum == (int) (crc32.getValue());
        assert pos == size : "pos: " + pos + ", getSerializeSize: " + size;
        return new DataBlock(size, result);
    }
}
