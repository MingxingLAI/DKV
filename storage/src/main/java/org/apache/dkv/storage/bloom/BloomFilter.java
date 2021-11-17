package org.apache.dkv.storage.bloom;

import org.apache.dkv.storage.bytes.Bytes;

public final class BloomFilter {
    
    private final int k;
    
    private final int bitsPerKey;
    
    private int bitLen;
    
    private byte[] result;

    public BloomFilter(final int k, final int bitsPerKey) {
        this.k = k;
        this.bitsPerKey = bitsPerKey;
    }

    /**
     * generate bloom filter.
     * @param keys multiple keys.
     * @return a byte array representing bloom filters.
     */
    public byte[] generate(final byte[][] keys) {
        assert keys != null;
        bitLen = keys.length * bitsPerKey;
        // align the bitLen.
        bitLen = ((bitLen + 7) / 8) << 3;
        bitLen = Math.max(bitLen, 64);
        result = new byte[bitLen >> 3];
        for (byte[] key : keys) {
            assert key != null;
            int h = Bytes.hash(key);
            for (int t = 0; t < k; t++) {
                int idx = (h % bitLen + bitLen) % bitLen;
                result[idx / 8] |= 1 << (idx % 8);
                int delta = (h >> 17) | (h << 15);
                h += delta;
            }
        }
        return result;
    }

    /**
     * check is key in storage.
     * @param key
     * @return true if key exists.
     */
    public boolean contains(final byte[] key) {
        assert result != null;
        int h = Bytes.hash(key);
        for (int t = 0; t < k; t++) {
            int idx = (h % bitLen + bitLen) % bitLen;
            if ((result[idx / 8] & (1 << (idx % 8))) == 0) {
                return false;
            }
            int delta = (h >> 17) | (h << 15);
            h += delta;
        }
        return true;
    }
}
