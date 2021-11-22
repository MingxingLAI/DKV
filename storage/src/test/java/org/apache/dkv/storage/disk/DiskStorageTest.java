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

package org.apache.dkv.storage.disk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.iterator.SeekIterator;
import org.apache.dkv.storage.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DiskStorageTest {

    private final TemporaryFolder folder = new TemporaryFolder();

    private DiskStorage diskStorage;

    @Before
    public void setUp() {
        try {
            folder.create();
            diskStorage = new DiskStorage(folder.getRoot().getAbsolutePath(), 10);
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }
    
    @Test
    public void testGetNextTableFileName() throws IOException {
        diskStorage.open();
        assertThat(diskStorage.getMaxTableId(), equalTo(-1));
        assertThat(diskStorage.getNexTableFileName(), equalTo(folder.getRoot() + File.separator + "SSTable00.sst"));
        assertThat(diskStorage.getNexTableFileName(), equalTo(folder.getRoot() + File.separator + "SSTable01.sst"));
        assertThat(diskStorage.getNexTableFileName(), equalTo(folder.getRoot() + File.separator + "SSTable02.sst"));
        assertThat(diskStorage.getNexTableFileName(), equalTo(folder.getRoot() + File.separator + "SSTable03.sst"));
        assertThat(diskStorage.getNexTableFileName(), equalTo(folder.getRoot() + File.separator + "SSTable04.sst"));
    }

    @Test
    public void testGetTables() throws IOException {
        diskStorage.open();
        assertThat(diskStorage.getTables(), equalTo(Collections.emptyList()));

        diskStorage.addTable(mock(SSTable.class));
        diskStorage.addTable(mock(SSTable.class));
        diskStorage.addTable(mock(SSTable.class));
        diskStorage.addTable(mock(SSTable.class));
        assertThat(diskStorage.getTables().size(), equalTo(4));
    }
    
    @Test
    public void testNextTableId() throws IOException {
        diskStorage.open();
        assertThat(diskStorage.nexTableId(), equalTo(0));
        assertThat(diskStorage.nexTableId(), equalTo(1));
        assertThat(diskStorage.nexTableId(), equalTo(2));
        assertThat(diskStorage.nexTableId(), equalTo(3));
    }
    
    @Test
    public void testRemoveObsoleteTables() throws IOException {
        diskStorage.open();
        List<SSTable> tables = Arrays.asList(mock(SSTable.class), mock(SSTable.class), mock(SSTable.class));
        assertThat(diskStorage.getTables().size(), equalTo(0));
        tables.forEach(diskStorage::addTable);
        assertThat(diskStorage.getTables().size(), equalTo(3));
        diskStorage.removeObsoleteTables(tables.subList(0, 1));
        assertThat(diskStorage.getTables().size(), equalTo(2));
    }
    
    @Test
    public void testIterator() throws IOException {
        diskStorage.open();
        TestUtil.createSSTables(diskStorage);
        int count = 0;
        SeekIterator<KeyValuePair> iterator = diskStorage.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count, equalTo(12));
    }

    @Test
    public void testIteratorWithArgs() throws IOException {
        diskStorage.open();
        TestUtil.createSSTables(diskStorage);
        int count = 0;
        SeekIterator<KeyValuePair> iterator = diskStorage.iterator(diskStorage.getTables());
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        assertThat(count, equalTo(12));
    }
}
