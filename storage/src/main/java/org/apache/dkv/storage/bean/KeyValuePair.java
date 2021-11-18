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

package org.apache.dkv.storage.bean;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Comparator;
import lombok.Getter;
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.bytes.BytesBuilder;

@Getter
public final class KeyValuePair implements Comparable<KeyValuePair> {

    public static final int RAW_KEY_LEN_SIZE = 4;

    public static final int VAL_LEN_SIZE = 4;

    public static final int OP_SIZE = 1;

    public static final int SEQ_ID_SIZE = 8;

    private final byte[] key;

    private final byte[] value;

    private final OperationType operationType;

    private final long sequenceId;

    public enum OperationType {
        Put((byte) 0),
        Delete((byte) 1);

        private final byte code;

        OperationType(final byte code) {
            this.code = code;
        }

        public static OperationType convertCodeToOperationType(final byte code) {
            switch (code) {
                case 0:
                    return Put;
                case 1:
                    return Delete;
                default:
                    throw new IllegalArgumentException("Unknown code: " + code);
            }
        }

        public byte getCode() {
            return this.code;
        }
    }

    private KeyValuePair(final byte[] key, final byte[] value, final OperationType operationType, final long sequenceId) {
        assert key != null;
        assert value != null;
        assert operationType != null;
        assert sequenceId >= 0;
        this.key = key;
        this.value = value;
        this.operationType = operationType;
        this.sequenceId = sequenceId;
    }

    public static KeyValuePair create(final byte[] key, final byte[] value, final OperationType operationType, final long sequenceId) {
        return new KeyValuePair(key, value, operationType, sequenceId);
    }

    public static KeyValuePair createPut(final byte[] key, final byte[] value, final long sequenceId) {
        return KeyValuePair.create(key, value, OperationType.Put, sequenceId);
    }

    public static KeyValuePair createDelete(final byte[] key, final long sequenceId) {
        return KeyValuePair.create(key, Bytes.EMPTY_BYTES, OperationType.Delete, sequenceId);
    }

    private int getRawKeyLen() {
        return key.length + OP_SIZE + SEQ_ID_SIZE;
    }

    public byte[] toBytes() {
        BytesBuilder builder = new BytesBuilder(getSerializeSize());
        // Encode raw key length
        int rawKeyLen = getRawKeyLen();
        byte[] rawKeyLenBytes = Bytes.toBytes(rawKeyLen);
        builder.append(rawKeyLenBytes);
        
        // Encode value length.
        byte[] valLen = Bytes.toBytes(value.length);
        builder.append(valLen);
        
        // Encode key
        builder.append(key);
        
        // Encode Op
        builder.append(new byte[] {operationType.getCode()});
        
        // Encode sequenceId
        byte[] seqIdBytes = Bytes.toBytes(sequenceId);
        builder.append(seqIdBytes);

        // Encode value
        builder.append(value);
        return builder.getBuffer();
    }

    @Override
    public int compareTo(final KeyValuePair kv) {
        if (null == kv) {
            throw new IllegalArgumentException("kv to compare should be null");
        }
        int ret = Bytes.compare(this.key, kv.key);
        if (ret != 0) {
            return ret;
        }
        if (this.sequenceId != kv.sequenceId) {
            return this.sequenceId > kv.sequenceId ? -1 : 1;
        }
        if (this.operationType != kv.operationType) {
            return this.operationType.getCode() > kv.operationType.getCode() ? -1 : 1;
        }
        return 0;
    }

    @Override
    public boolean equals(final Object kv) {
        if (null == kv) {
            return false;
        }
        if (!(kv instanceof KeyValuePair)) {
            return false;
        }
        KeyValuePair that = (KeyValuePair) kv;
        return compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public int getSerializeSize() {
        return RAW_KEY_LEN_SIZE + VAL_LEN_SIZE + getRawKeyLen() + value.length;
    }

    @Override
    public String toString() {
        return "key=" + Bytes.toHex(this.key) + "/op=" + operationType + "/sequenceId=" + this.sequenceId + "/value=" + Bytes.toHex(this.value);
    }

    public static KeyValuePair parseFrom(final byte[] bytes, final int offset) throws IOException {
        Preconditions.checkNotNull(bytes, "buff is null");
        if (offset + RAW_KEY_LEN_SIZE + VAL_LEN_SIZE >= bytes.length) {
            throw new IOException("Invalid offset or len. offset: " + offset + ", len: " + bytes.length);
        }
        // Decode raw key length
        int pos = offset;
        final int rawKeyLen = Bytes.toInt(Bytes.slice(bytes, pos, RAW_KEY_LEN_SIZE));
        pos += RAW_KEY_LEN_SIZE;

        // Decode value length
        final int valLen = Bytes.toInt(Bytes.slice(bytes, pos, VAL_LEN_SIZE));
        pos += VAL_LEN_SIZE;

        // Decode key
        int keyLen = rawKeyLen - OP_SIZE - SEQ_ID_SIZE;
        final byte[] key = Bytes.slice(bytes, pos, keyLen);
        pos += keyLen;

        // Decode Op
        OperationType operationType = OperationType.convertCodeToOperationType(bytes[pos]);
        pos += 1;

        // Decode sequenceId
        long sequenceId = Bytes.toLong(Bytes.slice(bytes, pos, SEQ_ID_SIZE));
        pos += SEQ_ID_SIZE;

        // Decode value.
        byte[] val = Bytes.slice(bytes, pos, valLen);
        return create(key, val, operationType, sequenceId);
    }

    public static KeyValuePair parseFrom(final byte[] bytes) throws IOException {
        return parseFrom(bytes, 0);
    }

    private static class KeyValueComparator implements Comparator<KeyValuePair> {

        @Override
        public int compare(final KeyValuePair a, final KeyValuePair b) {
            if (a == b) {
                return 0;
            }
            if (null == a) {
                return -1;
            }
            if (null == b) {
                return 1;
            }
            return a.compareTo(b);
        }
    }
}
