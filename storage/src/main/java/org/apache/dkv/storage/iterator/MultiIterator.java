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

package org.apache.dkv.storage.iterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.dkv.storage.bean.KeyValuePair;

/**
 * iterator to visit multiple iterators.
 */
public class MultiIterator implements SeekIterator<KeyValuePair> {
    
    private final SeekIterator<KeyValuePair>[] iterators;
    
    private final Queue<SortItem> queue;
    
    public MultiIterator(final SeekIterator<KeyValuePair>[] iterators) throws IOException {
        assert iterators != null;
        this.iterators = iterators;
        this.queue = new PriorityQueue<>(iterators.length, Comparator.comparing(SortItem::getKeyValuePair));
        for (SeekIterator<KeyValuePair> iterator : iterators) {
            // Only add iterator into queue when this iterator is not empty
            if (null != iterator && iterator.hasNext()) {
                queue.add(new SortItem(iterator.next(), iterator));
            }
        }
    }
    
    public MultiIterator(final List<SeekIterator<KeyValuePair>> iterators) throws IOException {
        this(iterators.toArray(new SeekIterator[0]));
    }

    /**
     * 
     * @param kv seek to specific element.
     * @throws IOException IO Exception.
     */
    @Override
    public void seekTo(final KeyValuePair kv) throws IOException {
        queue.clear();
        for (SeekIterator<KeyValuePair> iter : iterators) {
            iter.seekTo(kv);
            if (iter.hasNext()) {
                // Only the iterator which hash some elements should be enqueued.
                queue.add(new SortItem(iter.next(), iter));
            }
        }
    }

    /**
     * 
     * @return true if has next element.
     */
    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    /**
     * 
     * @return next element.
     * @throws IOException IO Exception.
     */
    @Override
    public KeyValuePair next() throws IOException {
        while (!queue.isEmpty()) {
            SortItem item = queue.poll();
            if (null != item.getKeyValuePair() && null != item.getIter()) {
                if (item.iter.hasNext()) {
                    queue.add(new SortItem(item.getIter().next(), item.iter));
                }
                return item.keyValuePair;
            }
        }
        return null;
    }

    @AllArgsConstructor
    @Getter
    private static class SortItem {

        private final KeyValuePair keyValuePair;

        private final SeekIterator<KeyValuePair> iter;
    }
}
