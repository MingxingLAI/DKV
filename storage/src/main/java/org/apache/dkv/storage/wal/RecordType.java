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

package org.apache.dkv.storage.wal;

import lombok.AllArgsConstructor;
import lombok.Getter;

// WAL log type
@AllArgsConstructor
@Getter
public enum RecordType {
    
    // Zero is preserved for preallocated files
    ZeroType((byte) 0),
    
    FullType((byte) 1),
    
    FirstType((byte) 2),
    
    MiddleType((byte) 3),
    
    LastType((byte) 4);

    @Getter
    private static final int MAX_RECORD_TYPE = LastType.code;
    
    private final byte code;
    
    public static RecordType convertCodeToRecordType(final byte code) {
        switch (code) {
            case 0:
                return ZeroType;
            case 1:
                return FullType;
            case 2:
                return FirstType;
            case 3:
                return MiddleType;
            case 4:
                return LastType;
            default:
                throw new IllegalArgumentException("Unknown code: " + code);
        }
    }
}
