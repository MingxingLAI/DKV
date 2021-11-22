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

package org.apache.dkv.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.compact.Compactor;
import org.apache.dkv.storage.compact.DefaultCompactor;
import org.apache.dkv.storage.config.Config;
import org.apache.dkv.storage.disk.DiskStorage;
import org.apache.dkv.storage.flush.DefaultFlusher;
import org.apache.dkv.storage.iterator.Iterator;
import org.apache.dkv.storage.iterator.MultiIterator;
import org.apache.dkv.storage.iterator.ScanIterator;
import org.apache.dkv.storage.iterator.SeekIterator;
import org.apache.dkv.storage.memory.MemStore;

/**
 * Distributed key-value database
 */
public final class DKV implements Closeable {
    
    private MemStore memStore;
    
    private DiskStorage diskStorage;

    private AtomicLong sequenceId;
    
    private final Config config;

    private DKV(final Config conf) {
        this.config = conf;
    }

    /**
     * create database.
     * @param config config
     * @return database object
     */
    public static DKV create(final Config config) {
        return new DKV(config);
    }

    /**
     * create database.
     * @return database object
     */
    public static DKV create() {
        return create(Config.getDefault());
    }

    /**
     * open database.
     * @return database object
     * @throws IOException IO Exception
     */
    public DKV open() throws IOException {
        assert config != null;
        
        // initialize the disk store
        diskStorage = new DiskStorage(config.getDataDir(), config.getMaxDiskFiles());
        diskStorage.open();
        // TODO initialize the max sequence id here.
        this.sequenceId = new AtomicLong(0);
        
        // initialize the MemStore
        ExecutorService pool = Executors.newFixedThreadPool(config.getMaxThreadPoolSize());
        this.memStore = new MemStore(config, new DefaultFlusher(diskStorage), pool);
        
        // initialize the compactor
        Compactor compactor = new DefaultCompactor(diskStorage);
        pool.submit(compactor);
        
        return this;
    }
    
    public void put(final byte[] key, final byte[] value) throws IOException {
        memStore.add(KeyValuePair.createPut(key, value, sequenceId.incrementAndGet()));
    }
    
    public void delete(final byte[] key) throws IOException {
        memStore.add(KeyValuePair.createDelete(key, sequenceId.incrementAndGet()));
    }

    /**
     * get specific key
     * @param key byte array of key
     * @return key value pair
     * @throws IOException IO Exception
     */
    public KeyValuePair get(final byte[] key) throws IOException {
        Iterator<KeyValuePair> it = scan(key, Bytes.EMPTY_BYTES);
        if (it.hasNext()) {
            KeyValuePair keyValuePair = it.next();
            if (Bytes.compare(keyValuePair.getKey(), key) == 0) {
                return keyValuePair;
            }
        }
        return null;
    }

    /**
     * Scan database
     * @param start start point
     * @param stop stop point
     * @return iterator to traverse database
     * @throws IOException IO Exception
     */
    public Iterator<KeyValuePair> scan(final byte[] start, final byte[] stop) throws IOException {
        List<SeekIterator<KeyValuePair>> iterators = new ArrayList<>();
        iterators.add(memStore.iterator());
        iterators.add(diskStorage.iterator());

        MultiIterator multiIterator = new MultiIterator(iterators);
        
        // with start being EMPTY_BYTES means min infinity, will skip to seek
        if (Bytes.compare(start, Bytes.EMPTY_BYTES) != 0) {
            multiIterator.seekTo(KeyValuePair.createDelete(start, sequenceId.get()));
        }
        KeyValuePair stopKv = null;
        if (Bytes.compare(stop, Bytes.EMPTY_BYTES) != 0) {
            stopKv = KeyValuePair.createDelete(stop, Long.MAX_VALUE);
        }
        return new ScanIterator(stopKv, multiIterator);
    }

    /**
     * scan database
     * @return iterator
     * @throws IOException IO Exception
     */
    public Iterator<KeyValuePair> scan() throws IOException {
        return scan(Bytes.EMPTY_BYTES, Bytes.EMPTY_BYTES);
    }

    @Override
    public void close() throws IOException {
        memStore.close();
        diskStorage.close();
    }
}
