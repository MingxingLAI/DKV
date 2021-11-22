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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.Getter;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.block.DataBlock;
import org.apache.dkv.storage.block.IndexBlock;
import org.apache.dkv.storage.block.TailerBlock;
import org.apache.dkv.storage.iterator.SeekIterator;

/**
 * SSTable object, It's read a SSTable file.
 */
public final class SSTable implements Closeable {
    
    @Getter
    private final String fileName;
    
    private final long fileSize;
    
    @Getter
    private final TailerBlock tailerBlock;
    
    private final RandomAccessFile in;
    
    @Getter
    private final SortedSet<IndexBlock> indexBlocks;

    /**
     * open a SSTable file, read it's tailer block and index blocks.
     * @param fileName file name ready to read
     * @throws IOException IO Exception.
     */
    public SSTable(final String fileName) throws IOException {
        this.fileName = fileName;
        File f = new File(fileName);
        this.fileSize = f.length();
        this.in = new RandomAccessFile(f, "r");

        // read tailer block from SSTable file
        tailerBlock = readTailerBlock();
        // read index blocks from SSTable file
        indexBlocks = readIndexBlocks();
    }
    
    private TailerBlock readTailerBlock() throws IOException {
        assert this.fileSize > TailerBlock.TAILER_SIZE;
        // seek to tailer block
        in.seek(fileSize - TailerBlock.TAILER_SIZE);
        // read tailer block
        byte[] buffer = new byte[TailerBlock.TAILER_SIZE];
        assert in.read(buffer) == TailerBlock.TAILER_SIZE;
        return TailerBlock.parseFrom(buffer, 0);
    }
    
    private SortedSet<IndexBlock> readIndexBlocks() throws IOException {
        // TODO maybe a large memory, and overflow
        byte[] buffer = new byte[(int) tailerBlock.getIndexBlockSize()];
        in.seek(tailerBlock.getIndexBlockOffset());
        assert in.read(buffer) == tailerBlock.getIndexBlockSize();
        int offset = 0;
        SortedSet<IndexBlock> indexBlocks = new TreeSet<>();
        do {
            IndexBlock indexBlock = IndexBlock.parseFrom(buffer, offset);
            offset += indexBlock.getSerializeSize();
            indexBlocks.add(indexBlock);
        } while (offset < buffer.length);
        assert indexBlocks.size() == tailerBlock.getBlockCount();
        return indexBlocks;
    }
    
    private DataBlock load(final IndexBlock indexBlock) throws IOException {
        in.seek(indexBlock.getBlockOffset());
        
        // TODO maybe overflow
        byte[] buffer = new byte[(int) indexBlock.getBlockSize()];
        assert in.read(buffer) == buffer.length;
        return DataBlock.parseFrom(buffer, 0, buffer.length);
    }
    
    @Override
    public void close() throws IOException {
        if (null != in) {
            in.close();
        }
    }

    /**
     * create a iterator to visit SSTable.
     * 
     * @return iterator
     */
    public SeekIterator<KeyValuePair> iterator() {
        return new InternalIterator();
    }
    
    private final class InternalIterator implements SeekIterator<KeyValuePair> {
            
        private int currentKvIndex;
        
        private DataBlock currentDataBlock;
        
        private java.util.Iterator<IndexBlock> indexBlockIterator;
    
        InternalIterator() {
            currentDataBlock = null;
            indexBlockIterator = indexBlocks.iterator();
        }
        
        private boolean nextDataBlock() throws IOException {
            if (!indexBlockIterator.hasNext()) {
                return false;
            }

            currentDataBlock = load(indexBlockIterator.next());
            currentKvIndex = 0;
            return true;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (null == currentDataBlock) {
                return nextDataBlock();
            } else {
                if (currentKvIndex < currentDataBlock.getKeyValuePairs().size()) {
                    return true;
                } else {
                    return nextDataBlock();
                }
            }
        }

        @Override
        public KeyValuePair next() {
            return currentDataBlock.getKeyValuePairs().get(currentKvIndex++);
        }

        @Override
        public void seekTo(final KeyValuePair target) throws IOException {
            // Locate the smallest index block which has the lastKv >= target
            indexBlockIterator = indexBlocks.tailSet(IndexBlock.createSeekDummy(target)).iterator();
            currentDataBlock = null;
            if (indexBlockIterator.hasNext()) {
                currentDataBlock = load(indexBlockIterator.next());
                // Locate the smallest Key Value pair which is greater than or equals to the given key value pair.
                // We're sure that we can find the currentKvIndex, because lastKv of the block is greater than or equals
                // to the target Key Value pair.
                currentKvIndex = 0;
                for (KeyValuePair keyValuePair : currentDataBlock.getKeyValuePairs()) {
                    if (keyValuePair.compareTo(target) >= 0) {
                        break;
                    }
                    currentKvIndex++;
                }

                if (currentKvIndex >= currentDataBlock.getKeyValuePairs().size()) {
                    throw new IOException("Data block mis-encoded, lastKV of the currentReader >= kv, but "
                            + "we found all kv < target");
                }
            }
        }
    }
    
}
