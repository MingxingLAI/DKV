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
import org.apache.dkv.storage.bytes.Bytes;
import org.apache.dkv.storage.bytes.BytesBuilder;

@AllArgsConstructor
@Getter
public class RecordHeader {
    
    private final int checksum;
    
    private final RecordType type;
    
    private final int length;
    
    /**
     * serialize a record header to byte array.
     * @return byte array represent record header.
     */
    public byte[] serialize() {
        BytesBuilder builder = new BytesBuilder(WALWriter.getHEADER_SIZE());

        // encode checksum(4 bytes)
        byte[] bytes = Bytes.toBytes(checksum);
        builder.append(bytes);

        // encode record type(1 bytes)
        bytes = Bytes.toBytes(type);
        builder.append(bytes);

        // encode record length(2 bytes)
        bytes = Bytes.toTwoBytes(length);
        builder.append(bytes);
        
        return builder.getBuffer();
    }
    
    /**
     * parse record header from byte array.
     * @param buf byte buffer
     * @param offset offset of record header in WALFile
     * @return record header object
     */
    public static RecordHeader parseFrom(final byte[] buf, final int offset) {
        int pos = offset;

        // decode checksum(4 bytes)
        final int checksum = Bytes.toInt(Bytes.slice(buf, pos, 4));
        pos += 4;

        // decode record type(1 bytes)
        final RecordType recordType = RecordType.convertCodeToRecordType(buf[pos]);
        pos += 1;

        // decode length(2 bytes)
        final int length = Bytes.twoBytesToInt(Bytes.slice(buf, pos, 2));
        
        return new RecordHeader(checksum, recordType, length);
    }

}
