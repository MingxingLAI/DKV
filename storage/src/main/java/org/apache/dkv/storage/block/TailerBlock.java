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

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.bytes.BytesBuilder;

/**
 * tailer block in SSTable, It's the last block of SSTable.
 */
@Getter
@AllArgsConstructor
public final class TailerBlock {
    
    public static final long DISK_FILE_MAGIC = 0xFAC881234221FFA9L;

    private static final int FILE_SIZE = 8;
    
    private static final int COUNT_SIZE = 4;
    
    private static final int OFFSET_SIZE = 8;
    
    private static final int SIZE_SIZE = 8;
    
    private static final int MAGIC_SIZE = 8;
    
    public static final int TAILER_SIZE = FILE_SIZE + COUNT_SIZE + OFFSET_SIZE + SIZE_SIZE + MAGIC_SIZE;
    
    private final long fileSize;
    
    private final int blockCount;
    
    private final long indexBlockOffset;
    
    private final long indexBlockSize;
    
    private final long magicNumber;
    
    public TailerBlock(final long fileSize, final int blockCount, final long indexBlockOffset, final long indexBlockSize) {
        this.fileSize = fileSize;
        this.blockCount = blockCount;
        this.indexBlockOffset = indexBlockOffset;
        this.indexBlockSize = indexBlockSize;
        this.magicNumber = DISK_FILE_MAGIC;
    }

    /**
     * serialize a tailer block to byte array.
     * @return byte array represent tailer block.
     */
    public byte[] serialize() {
        BytesBuilder builder = new BytesBuilder(TAILER_SIZE);

        // encode file size(8 bytes)
        byte[] bytes = Bytes.toBytes(fileSize);
        builder.append(bytes);
        
        // encode block count(4 bytes)
        bytes = Bytes.toBytes(blockCount);
        builder.append(bytes);
        
        // encode index block offset(8 bytes)
        bytes = Bytes.toBytes(indexBlockOffset);
        builder.append(bytes);
        
        // encode index block size(8 bytes)
        bytes = Bytes.toBytes(indexBlockSize);
        builder.append(bytes);
        
        // encode magic number(8 bytes)
        bytes = Bytes.toBytes(DISK_FILE_MAGIC);
        builder.append(bytes);

        return builder.getBuffer();
    }
    
    /**
     * parse tailer block from byte array.
     * @param buf byte buffer
     * @param offset offset of tailer block in SSTable
     * @return tailer block object
     */
    public static TailerBlock parseFrom(final byte[] buf, final int offset) {
        int pos = offset;

        // decode file size(8 bytes)
        final long fileSize = Bytes.toLong(Bytes.slice(buf, pos, FILE_SIZE));
        pos += 8;
        
        // decode block count(4 bytes)
        final int blockCount = Bytes.toInt(Bytes.slice(buf, pos, COUNT_SIZE));
        pos += 4;
        
        // decode index block offset(8 bytes)
        final long dataBlockMetaOffset = Bytes.toLong(Bytes.slice(buf, pos, OFFSET_SIZE));
        pos += 8;
        
        // decode index block size(8 bytes)
        final long dataBlockMetaSize = Bytes.toLong(Bytes.slice(buf, pos, SIZE_SIZE));
        pos += 8;
        
        // decode magic number(8 bytes)
        final long magicNumber = Bytes.toLong(Bytes.slice(buf, pos, MAGIC_SIZE));
        assert DISK_FILE_MAGIC == magicNumber;

        return new TailerBlock(fileSize, blockCount, dataBlockMetaOffset, dataBlockMetaSize, magicNumber);
    }
}
