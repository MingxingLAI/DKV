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

package org.apache.dkv.storage.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bytes.Bytes;

/**
 * utility for unit test
 */
public class TestUtil {

    /**
     * generate skip list
     * @param data key list
     * @return skip list
     */
    public static ConcurrentSkipListMap<KeyValuePair, KeyValuePair> createKeyValuePairMap(final List<String> data) {
        Collection<KeyValuePair> keyValuePairs = data.stream().map(each -> KeyValuePair.createPut(Bytes.toBytes(each), Bytes.toBytes(each), 1)).collect(Collectors.toSet());
        ConcurrentSkipListMap<KeyValuePair, KeyValuePair> result = new ConcurrentSkipListMap<>();
        for (KeyValuePair keyValuePair: keyValuePairs) {
            result.put(keyValuePair, keyValuePair);
        }
        return result;
    }
    
}
