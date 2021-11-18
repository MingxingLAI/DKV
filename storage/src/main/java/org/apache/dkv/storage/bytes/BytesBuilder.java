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

import lombok.Getter;

@Getter
public final class BytesBuilder {

    private int pos;

    private final byte[] buffer;

    public BytesBuilder(final int size) {
        pos = 0;
        buffer = new byte[size];
    }

    /**
     * append bytes to buffer.
     * @param bytes
     */
    public void append(final byte[] bytes) {
        System.arraycopy(bytes, 0, buffer, pos, bytes.length);
        pos += bytes.length;
    }
}
