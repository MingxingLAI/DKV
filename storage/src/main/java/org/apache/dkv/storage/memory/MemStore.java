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

package org.apache.dkv.storage.memory;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.config.Config;
import org.apache.dkv.storage.flush.Flusher;
import org.apache.dkv.storage.iterator.IteratorWrapper;
import org.apache.dkv.storage.iterator.MemStoreIterator;
import org.apache.dkv.storage.iterator.SeekIterator;

@Slf4j
public final class MemStore {
    
    private final AtomicLong dataSize = new AtomicLong();
    
    @Getter
    private volatile ConcurrentSkipListMap<KeyValuePair, KeyValuePair> kvMap;
    
    @Getter
    private volatile ConcurrentSkipListMap<KeyValuePair, KeyValuePair> snapshot;
    
    private final ReentrantReadWriteLock updateLock = new ReentrantReadWriteLock();
    
    private final AtomicBoolean isSnapshotFlushing = new AtomicBoolean(false);
    
    private final ExecutorService pool;
    
    private final Config conf;
    
    private final Flusher flusher;
    
    public MemStore(final Config conf, final Flusher flusher, final ExecutorService pool) {
        this.conf = conf;
        this.flusher = flusher;
        this.pool = pool;
        dataSize.set(0);
        kvMap = new ConcurrentSkipListMap<>();
        this.snapshot = null;
    }
    
    public void add(final KeyValuePair kv) throws IOException {
        flushIfNeeded(true);
        try {
            updateLock.readLock().lock();
            KeyValuePair prevKeyValuePair;
            if ((prevKeyValuePair = kvMap.put(kv, kv)) == null) {
                dataSize.addAndGet(kv.getSerializeSize());
            } else {
                // delete previous element if this is update operation
                dataSize.addAndGet(kv.getSerializeSize() - prevKeyValuePair.getSerializeSize());
            }
        } finally {
            updateLock.readLock().unlock();
        }
        flushIfNeeded(false);
    }

    /**
     * crate iterator to visit MemStore.
     * @return iterator.
     * @throws IOException
     */
    public SeekIterator<KeyValuePair> createIterator() throws IOException {
        return new MemStoreIterator(this);
    }
    
    private void flushIfNeeded(final boolean shouldBlockingUpdate) throws IOException {
        if (dataSize.get() > conf.getMaxMemstoreSize()) {
            if (isSnapshotFlushing.get() && shouldBlockingUpdate) {
                throw new IOException("Memstore is full, currentDataSize=" + dataSize.get() + "B, maxMemstoreSize=" + conf.getMaxMemstoreSize() + "B, please wait until the flushing is finished.");
            } else if (isSnapshotFlushing.compareAndSet(false, true)) {
                // submit a flush task
                pool.submit(new FlusherTask());
            }
        }
    }
    
    private class FlusherTask implements Runnable {

        @Override
        public void run() {
            try {
                updateLock.writeLock().lock();
                snapshot = kvMap;
                kvMap = new ConcurrentSkipListMap<>();
                dataSize.set(0);
            } finally {
                updateLock.writeLock().unlock();
            }
            flush();
        }
        
        private void flush() {
            boolean isSuccess = false;
            for (int i = 0; i < conf.getFlushMaxRetries(); i++) {
                try {
                    flusher.flush(new IteratorWrapper(snapshot));
                    isSuccess = true;
                } catch (Exception e) {
                    log.error("Failed to flush memstore, retries= {} , maxFlushRetries= {}, {}", i, conf.getFlushMaxRetries(), e);
                    if (i >= conf.getFlushMaxRetries()) {
                        break;
                    }
                }
            }
            // clean the snapshot
            if (isSuccess) {
                snapshot = null;
                isSnapshotFlushing.compareAndSet(true, false);
            }
        }
    }
}
