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

package org.apache.dkv.storage.bytes;

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;

public final class Bytes {

    public static final byte[] EMPTY_BYTES = new byte[0];
    
    public static final String HEX_TMP = "0123456789ABCDEF";

    public static byte[] toBytes(final byte b) {
        return new byte[]{b};
    }

    public static byte[] toBytes(final String s) {
        if (null == s) {
            return new byte[0];
        }
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(final int x) {
        byte[] b = new byte[4];
        b[3] = (byte) (x & 0xFF);
        b[2] = (byte) ((x >> 8) & 0xFF);
        b[1] = (byte) ((x >> 16) & 0xFF);
        b[0] = (byte) ((x >> 24) & 0xFF);
        return b;
    }

    public static byte[] toBytes(final long x) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            int j = (7 - i) << 3;
            b[i] = (byte) ((x >> j) & 0xFF);
        }
        return b;
    }

    public static byte[] toBytes(final byte[] a, final byte[] b) {
        if (null == a) {
            return b;
        }
        if (null == b) {
            return a;
        }
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
    
    public static String toHex(final byte[] buf) {
        return toHex(buf, 0, buf.length);
    }

    public static String toHex(final byte[] buf, final int offset, final int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = offset; i < offset + len; i++) {
            int x = buf[i];
            if (x > 32 && x < 127) {
                sb.append((char) x);
            } else {
                sb.append("\\x").append(HEX_TMP.charAt((x >> 4) & 0x0F)).append(HEX_TMP.charAt(x & 0x0F));
            }
        }
        return sb.toString();
    }
    
    public static int toInt(final byte[] a) {
        int firstByte = (a[0] << 24) & 0xFF000000;
        int secondByte = (a[1] << 16) & 0x00FF0000;
        int thirdByte = (a[2] << 8) & 0x0000FF00;
        int fourthByte = (a[3]) & 0x000000FF;
        return firstByte | secondByte | thirdByte | fourthByte;
    }

    public static long toLong(final byte[] a) {
        long x = 0;
        for (int i = 0; i < 8; i++) {
            int j = (7 - i) << 3;
            long result = (0xFFL << j) & ((long) a[i] << j);
            x |= result;
        }
        return x;
    }

    public static byte[] slice(final byte[] buf, final int offset, final int len) {
        Preconditions.checkNotNull(buf, "buffer is null");
        Preconditions.checkArgument(offset >= 0 && len >= 0, "Invalid offset: " + offset + " or len: " + len);
        Preconditions.checkArgument(offset + len <= buf.length, "Buffer overflow, offset: " + offset + ", len: " + len + ", buf.length:" + buf.length);
        byte[] result = new byte[len];
        System.arraycopy(buf, offset, result, 0, len);
        return result;
    }

    public static int hash(final byte[] key) {
        if (null == key) {
            return 0;
        }
        int h = 1;
        for (byte b : key) {
            h = (h << 5) + h + b;
        }
        return h;
    }

    public static int compare(final byte[] a, final byte[] b) {
        if (a == b) {
            return 0;
        }
        if (null == a) {
            return -1;
        }
        if (null == b) {
            return 1;
        }
        for (int i = 0, j = 0; i < a.length && j < b.length; i++, j++) {
            int x = a[i] & 0xFF;
            int y = b[i] & 0xFF;
            if (x != y) {
                return x - y;
            }
        }
        return a.length - b.length;
    }
}
