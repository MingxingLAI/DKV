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

package org.apache.dkv.block;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.apache.dkv.storage.block.TailerBlock;
import org.junit.Test;

public class TailerBlockTest {

    @Test
    public void testTailerBlock() {
        TailerBlock tailerBlock1 = new TailerBlock(100, 4, 50, 50);
        TailerBlock tailerBlock2 = TailerBlock.parseFrom(tailerBlock1.serialize(), 0);
        
        assertThat(tailerBlock2.getFileSize(), equalTo(tailerBlock1.getFileSize()));
        assertThat(tailerBlock2.getBlockCount(), equalTo(tailerBlock1.getBlockCount()));
        assertThat(tailerBlock2.getDataBlockMetaOffset(), equalTo(tailerBlock1.getDataBlockMetaOffset()));
        assertThat(tailerBlock2.getDataBlockMetaSize(), equalTo(tailerBlock1.getDataBlockMetaSize()));
        assertThat(tailerBlock2.getMagicNumber(), equalTo(tailerBlock1.getMagicNumber()));
    }
}
