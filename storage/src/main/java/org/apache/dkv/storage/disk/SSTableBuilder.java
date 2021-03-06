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

package org.apache.dkv.storage.disk;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import lombok.Getter;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.block.DataBlock;
import org.apache.dkv.storage.block.IndexBlockBuilder;
import org.apache.dkv.storage.block.TailerBlock;

/**
 * SSTable Builder
 */
public final class SSTableBuilder implements Closeable {

    private static final int BLOCK_SIZE_UP_LIMIT = 1024 * 1024 * 2;

    private long currentOffset;
    
    @Getter
    private long fileSize;
    
    @Getter
    private final IndexBlockBuilder indexBlockBuilder;
    
    @Getter
    private DataBlock currentDataBlock;

    private final FileOutputStream outputStream;

    @Getter
    private int blockCount;

    @Getter
    private TailerBlock tailerBlock;
            
    private long dataBlockMetaOffset;
    
    private long dataBlockMetaSize;
    
    public SSTableBuilder(final String fileName) throws IOException {
        File f = new File(fileName);
        boolean isExists = f.createNewFile();
        Preconditions.checkState(isExists, "%s is exists.", fileName);
        outputStream = new FileOutputStream(f, true);
        currentOffset = 0;
        indexBlockBuilder = new IndexBlockBuilder();
        currentDataBlock = new DataBlock();
    }

    /**
     * append Key value pair into SSTable.
     * @param keyValuePair to append to SSTable.
     * @throws IOException IO Exception.
     */
    public void append(final KeyValuePair keyValuePair) throws IOException {
        if (null == keyValuePair) {
            return;
        }
        
        assert keyValuePair.getSerializeSize() + DataBlock.KV_SIZE_LEN + DataBlock.CHECKSUM_LEN < BLOCK_SIZE_UP_LIMIT;
        if (!currentDataBlock.isEmpty()) {
            if (keyValuePair.getSerializeSize() + currentDataBlock.getSize() >= BLOCK_SIZE_UP_LIMIT) {
                switchNextDataBlock();        
            }
        }
        currentDataBlock.append(keyValuePair);
    }
    
    private void switchNextDataBlock() throws IOException {
        assert null != currentDataBlock.getLastKv();
        
        byte[] buffer = currentDataBlock.serialize();
        outputStream.write(buffer);
        // save index info into DataBlockMeta
        indexBlockBuilder.append(currentDataBlock.getLastKv(), currentOffset, buffer.length, currentDataBlock.getBloomFilter());
        
        currentOffset += buffer.length;
        blockCount += 1;
        
        // switch to the next block.
        currentDataBlock = new DataBlock();
    }

    /**
     * append index into SSTable.
     * @throws IOException IO Exception.
     */
    public void appendIndex() throws IOException {
        // add last Data Block into index
        if (!currentDataBlock.isEmpty()) {
            switchNextDataBlock();
        }
        
        byte[] buffer = indexBlockBuilder.serialize();
        dataBlockMetaOffset = currentOffset;
        dataBlockMetaSize = buffer.length;
        outputStream.write(buffer);
        
        // advance the offset for writing Footer block
        currentOffset += buffer.length;
    }

    /**
     * append tailer into SSTable.
     * @throws IOException IO Exception.
     */
    public void appendTailer() throws IOException {
        fileSize = currentOffset + TailerBlock.TAILER_SIZE;
        tailerBlock = new TailerBlock(fileSize, blockCount, dataBlockMetaOffset, dataBlockMetaSize);
        outputStream.write(tailerBlock.serialize());
    }
    
    @Override
    public void close() throws IOException {
        if (null != outputStream) {
            try {
                outputStream.flush();
                FileDescriptor fd = outputStream.getFD();
                fd.sync();
            } finally {
                outputStream.close();
            }
        }
    }
}
