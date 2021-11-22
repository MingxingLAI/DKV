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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.iterator.MultiIterator;
import org.apache.dkv.storage.iterator.SeekIterator;

/**
 * Persistent storage.
 */
@Slf4j
public final class DiskStorage implements Closeable {

    public static final String FILE_NAME_TMP_SUFFIX = ".tmp";

    public static final String FILE_NAME_ARCHIVE_SUFFIX = ".archive";

    // SSTable xx.sst
    private static final Pattern DATA_FILE_RE = Pattern.compile("SSTable([0-9]+)\\.sst");

    private final String dataDir;
    
    private final List<SSTable> tables;
    
    private volatile AtomicInteger maxFileId;
    
    public DiskStorage(final String dataDir, final int maxDiskFiles) {
        this.dataDir = dataDir;
        this.tables = new ArrayList<>(maxDiskFiles);
    }
    
    private File[] getAllTableFiles() {
        File dir = new File(this.dataDir);
        return dir.listFiles(each -> DATA_FILE_RE.matcher(each.getName()).matches());
    }

    /**
     * get max table file id.
     * @return table file id
     */
    public synchronized int getMaxTableId() {
        // TODO use manifest file to save max file id, do not to traverse the disk file.
        File[] files = getAllTableFiles();
        int maxFileId = -1;
        for (File f : files) {
            Matcher matcher = DATA_FILE_RE.matcher(f.getName());
            if (matcher.matches()) {
                maxFileId = Math.max(Integer.parseInt(matcher.group(1)), maxFileId);
            }
        }
        return maxFileId;
    }

    /**
     * get next table file id.
     * @return table file id
     */
    public synchronized int nexTableId() {
        return maxFileId.incrementAndGet();
    }

    /**
     * add SSTable object to disk
     * @param table SSTable object.
     */
    public void addTable(final SSTable table) {
        tables.add(table);
    }

    /**
     * add disk file.
     * @param fileName file name ready to read.
     * @throws IOException IO Exception
     */
    public synchronized void addTable(final String fileName) throws IOException {
        addTable(new SSTable(fileName));
    }

    /**
     * get next SSTable file name.
     * @return file name
     */
    public synchronized String getNexTableFileName() {
        return new File(this.dataDir, String.format("SSTable%02d.sst", nexTableId())).toString();
    }

    /**
     * open a database, load all SSTable file
     * @throws IOException IO Exception
     */
    public void open() throws IOException {
        File[] files = getAllTableFiles();
        for (File f: files) {
            addTable(f.getAbsolutePath());
        }
        maxFileId = new AtomicInteger(getMaxTableId());
    }
    
    public List<SSTable> getTables() {
        synchronized (tables) {
            return new ArrayList<>(tables);
        }
    }

    /**
     * remove SSTable table.
     * @param tables SSTable file to remove
     */
    public void removeObsoleteTables(final Collection<SSTable> tables) {
        synchronized (this.tables) {
            this.tables.removeAll(tables);
        }
    }
    
    @Override
    public void close() throws IOException {
        IOException closedException = null;
        for (SSTable table : tables) {
            try {
                table.close();
            } catch (IOException e) {
                closedException = e;
            }
        }
        if (null != closedException) {
            throw closedException;
        }
    }

    /**
     * iterator multiple SSTable files.
     * @param tables SSTable files
     * @return iterator to traverse multiple SSTable.
     * @throws IOException IO Exception
     */
    public SeekIterator<KeyValuePair> iterator(final List<SSTable> tables) throws IOException {
        List<SeekIterator<KeyValuePair>> iterators = new ArrayList<>(tables.size());
        tables.forEach(each -> iterators.add(each.iterator()));
        return new MultiIterator(iterators);
    }
    
    public SeekIterator<KeyValuePair> iterator() throws IOException {
        return iterator(getTables());
    }
}
