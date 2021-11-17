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

package org.apache.dkv.storage.iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.bytes.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class MultiIteratorTest {
    
    private static final KeyValuePair EXPECTED_KEY_VALUE_PAIR = KeyValuePair.createPut(Bytes.toBytes("00001"), Bytes.toBytes("00001"), 1L);
    
    @Test
    public void testMergeNormalIterator() throws IOException {
        int[] a = new int[] {2, 5, 8, 10, 20 };
        int[] b = new int[] {11, 12, 12 };
        MockIterator iter1 = new MockIterator(a);
        MockIterator iter2 = new MockIterator(b);
        SeekIterator[] iterators = new SeekIterator[] {iter1, iter2 };
        MultiIterator multiIter = new MultiIterator(iterators);

        String[] results = new String[] {"00002", "00005", "00008", "00010", "00011", "00012", "00012", "00020" };
        int index = 0;
        while (multiIter.hasNext()) {
            KeyValuePair kv = multiIter.next();
            assertThat(index, lessThan(results.length));
            assertThat(kv.getKey(), equalTo(Bytes.toBytes(results[index])));
            assertThat(kv.getValue(), equalTo(Bytes.toBytes(results[index])));
            index++;
        }
        assertThat(index, equalTo(results.length));
    }

    @Test
    public void testMergeEmptyIterator() throws IOException {
        int[] a = new int[] {};
        int[] b = new int[] {};
        MockIterator iter1 = new MockIterator(a);
        MockIterator iter2 = new MockIterator(b);
        SeekIterator<KeyValuePair>[] iterators = new SeekIterator[] {iter1, iter2 };
        MultiIterator multiIter = new MultiIterator(iterators);
        assertFalse(multiIter.hasNext());
    }

    @Test
    public void testMergeEmptyAndNormalIterator() throws IOException {
        int[] a = new int[]{};
        int[] b = new int[]{1};
        MockIterator iter1 = new MockIterator(a);
        MockIterator iter2 = new MockIterator(b);
        SeekIterator<KeyValuePair>[] iterators = new SeekIterator[]{iter1, iter2};
        MultiIterator multiIter = new MultiIterator(iterators);
        assertTrue(multiIter.hasNext());
        assertThat(multiIter.next(), equalTo(EXPECTED_KEY_VALUE_PAIR));
        assertFalse(multiIter.hasNext());
    }

    @Test
    public void testMergeMultipleIterator() throws IOException {
        int[] a = new int[] {};
        int[] b = new int[] {1, 1};
        int[] c = new int[] {1, 1};
        MockIterator iter1 = new MockIterator(a);
        MockIterator iter2 = new MockIterator(b);
        MockIterator iter3 = new MockIterator(c);
        SeekIterator[] iterators = new SeekIterator[] {iter1, iter2, iter3 };
        MultiIterator multiIter = new MultiIterator(iterators);

        int count = 0;
        while (multiIter.hasNext()) {
            assertThat(multiIter.next(), equalTo(EXPECTED_KEY_VALUE_PAIR));
            count++;
        }
        Assert.assertEquals(count, 4);
    }
    
    @Test
    public void testMergeMultipleIteratorByList() throws IOException {
        int[] a = new int[] {};
        int[] b = new int[] {1, 1};
        int[] c = new int[] {1, 1};
        MockIterator iter1 = new MockIterator(a);
        MockIterator iter2 = new MockIterator(b);
        MockIterator iter3 = new MockIterator(c);
        List<SeekIterator<KeyValuePair>> iterators = Arrays.asList(iter1, iter2, iter3);
        MultiIterator multiIter = new MultiIterator(iterators);

        int count = 0;
        while (multiIter.hasNext()) {
            assertThat(multiIter.next(), equalTo(EXPECTED_KEY_VALUE_PAIR));
            count++;
        }
        Assert.assertEquals(count, 4);
    }
    
    public static class MockIterator implements SeekIterator<KeyValuePair> {

        private int cur;

        private final KeyValuePair[] kvs;

        public MockIterator(final int[] array) {
            assert array != null;
            kvs = new KeyValuePair[array.length];
            for (int i = 0; i < array.length; i++) {
                String s = String.format("%05d", array[i]);
                kvs[i] = KeyValuePair.createPut(Bytes.toBytes(s), Bytes.toBytes(s), 1L);
            }
            cur = 0;
        }

        /**
         * 
         * @return true if has next element.
         */
        @Override
        public boolean hasNext() {
            return cur < kvs.length;
        }

        /**
         * 
         * @return next element.
         */
        @Override
        public KeyValuePair next() {
            return kvs[cur++];
        }

        /**
         * 
         * @param kv seek to specific element.
         */
        @Override
        public void seekTo(final KeyValuePair kv) {
            for (cur = 0; cur < kvs.length; cur++) {
                if (kvs[cur].compareTo(kv) >= 0) {
                    break;
                }
            }
        }
    }
}
