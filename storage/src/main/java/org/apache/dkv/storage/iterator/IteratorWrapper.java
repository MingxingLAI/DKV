package org.apache.dkv.storage.iterator;

import java.util.SortedMap;
import org.apache.dkv.storage.bean.KeyValuePair;

public final class IteratorWrapper implements SeekIterator<KeyValuePair> {

    private SortedMap<KeyValuePair, KeyValuePair> sortedMap;
    
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
