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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;

public class RecordHeaderTest {

    @Test
    public void testRecordHeader() {
        RecordHeader recordHeader = new RecordHeader(334876, RecordType.FirstType, 100);
        RecordHeader actual = RecordHeader.parseFrom(recordHeader.serialize(), 0);
        assertThat(actual.getChecksum(), equalTo(recordHeader.getChecksum()));
        assertThat(actual.getType(), equalTo(recordHeader.getType()));
        assertThat(actual.getLength(), equalTo(recordHeader.getLength()));
    }
}
