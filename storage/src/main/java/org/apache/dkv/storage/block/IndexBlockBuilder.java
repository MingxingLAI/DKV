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
import org.apache.dkv.storage.bean.KeyValuePair;

/**
 * serialize index block into bytes.
 */
public class IndexBlockBuilder {
    
    private final List<IndexBlock> indexBlocks = new ArrayList<>();
    
    private int totalBytes;

    /**
     * append a index block into writer.
     * @param lastKv lastKv of data block.
     * @param offset offset in data block.
     * @param size size of data block.
     * @param bloomFilter bloom filter of data block.
     */
    public void append(final KeyValuePair lastKv, final long offset, final long size, final byte[] bloomFilter) {
        IndexBlock indexBlock = new IndexBlock(lastKv, offset, size, bloomFilter);
        indexBlocks.add(indexBlock);
        totalBytes += indexBlock.getSerializeSize();
    }

    /**
     * serialize index block into bytes.
     * 
     * @return byte array represent DataBlockMeta
     * @throws IOException IO Exception.
     */
    public byte[] serialize() throws IOException {
        byte[] buffer = new byte[totalBytes];
        int pos = 0;
        for (IndexBlock indexBlock : indexBlocks) {
            byte[] bytes = indexBlock.toBytes();
            System.arraycopy(bytes, 0, buffer, pos, bytes.length);
            pos += bytes.length;
        }
        assert pos == totalBytes;
        return buffer;
    }
}
