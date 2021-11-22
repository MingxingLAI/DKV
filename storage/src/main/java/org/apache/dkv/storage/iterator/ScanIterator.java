package org.apache.dkv.storage.iterator;

import java.io.IOException;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bean.KeyValuePair.OperationType;
import org.apache.dkv.storage.bytes.Bytes;

/**
 * iterator to scan elements.
 */
public final class ScanIterator implements Iterator<KeyValuePair> {

    private final KeyValuePair stopKv;

    private final Iterator<KeyValuePair> iterator;

    private KeyValuePair lastKv;

    private KeyValuePair pendingKv;

    public ScanIterator(final KeyValuePair stopKv, final SeekIterator<KeyValuePair> iterator) {
        this.stopKv = stopKv;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (null == pendingKv) {
            switchToNewKey();
        }
        return null != pendingKv;
    }

    private boolean shouldStop(final KeyValuePair keyValuePair) {
        return null != stopKv && Bytes.compare(stopKv.getKey(), keyValuePair.getKey()) <= 0;
    }

    private void switchToNewKey() throws IOException {
        if (null != lastKv && shouldStop(lastKv)) {
            return;
        }
        findNextElement();
    }

    private void findNextElement() throws IOException {
        KeyValuePair currentKeyValuePair;
        while (iterator.hasNext()) {
            currentKeyValuePair = iterator.next();
            // if we reach the stop key-value 
            if (shouldStop(currentKeyValuePair)) {
                return;
            }
            // found valid data
            if (currentKeyValuePair.getOperationType() == OperationType.Put) {
                // lastKv is infinity or currentKeyValuePair less than lastKv
                if (null == lastKv || Bytes.compare(lastKv.getKey(), currentKeyValuePair.getKey()) < 0) {
                    lastKv = currentKeyValuePair;
                    pendingKv = currentKeyValuePair;
                    return;
                }
                // found obsolete data, just skip
            } else if (currentKeyValuePair.getOperationType() == OperationType.Delete) {
                if (null == lastKv || Bytes.compare(lastKv.getKey(), currentKeyValuePair.getKey()) != 0) {
                    lastKv = currentKeyValuePair;
                }
            } else {
                throw new IOException("Unknown op code: " + currentKeyValuePair.getOperationType());
            }
        }
    }

    @Override
    public KeyValuePair next() throws IOException {
        if (null == pendingKv) {
            switchToNewKey();
        }
        lastKv = pendingKv;
        pendingKv = null;
        return lastKv;
    }
}
