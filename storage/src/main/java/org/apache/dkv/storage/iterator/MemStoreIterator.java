package org.apache.dkv.storage.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.memory.MemStore;

public final class MemStoreIterator implements SeekIterator<KeyValuePair> {

    private final MultiIterator iterator;

    public MemStoreIterator(final MemStore memStore) throws IOException {
        List<IteratorWrapper> inputs = new ArrayList<>(2);
        createIteratorWrapper(memStore.getKvMap()).ifPresent(inputs::add);
        createIteratorWrapper(memStore.getSnapshot()).ifPresent(inputs::add);
        iterator = new MultiIterator(inputs.toArray(new IteratorWrapper[0]));
    }

    @Override
    public void seekTo(final KeyValuePair kv) throws IOException {
        iterator.seekTo(kv);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValuePair next() throws IOException {
        return iterator.next();
    }

    private Optional<IteratorWrapper> createIteratorWrapper(final ConcurrentSkipListMap<KeyValuePair, KeyValuePair> map) {
        if (null != map && !map.isEmpty()) {
            return Optional.of(new IteratorWrapper(map));
        } else {
            return Optional.empty();
        }
    }

    public static final class IteratorWrapper implements SeekIterator<KeyValuePair> {

        private final SortedMap<KeyValuePair, KeyValuePair> sortedMap;

        private java.util.Iterator<KeyValuePair> iterator;

        public IteratorWrapper(final SortedMap<KeyValuePair, KeyValuePair> sortedMap) {
            this.sortedMap = sortedMap;
            this.iterator = sortedMap.values().iterator();
        }

        @Override
        public void seekTo(final KeyValuePair kv) {
            iterator = sortedMap.tailMap(kv).values().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValuePair next() {
            return iterator.next();
        }
    }
}
