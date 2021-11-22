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

package org.apache.dkv.storage.flush;

import java.io.File;
import java.io.IOException;
import lombok.AllArgsConstructor;
import org.apache.dkv.storage.bean.KeyValuePair;
import org.apache.dkv.storage.disk.DiskStorage;
import org.apache.dkv.storage.disk.SSTableBuilder;
import org.apache.dkv.storage.iterator.Iterator;
import org.apache.dkv.storage.util.FileUtil;

@AllArgsConstructor
public final class DefaultFlusher implements Flusher {

    private final DiskStorage diskStorage;
    
    @Override
    public void flush(final Iterator<KeyValuePair> it) throws IOException {
        String fileName = diskStorage.getNexTableFileName();
        String tempFileName = fileName + DiskStorage.FILE_NAME_TMP_SUFFIX;
        
        try {
            performFlush(it, fileName, tempFileName);    
        } finally {
            File f = new File(tempFileName);
            if (f.exists()) {
                f.delete();
            }
        }
    }
    
    private void performFlush(final Iterator<KeyValuePair> iterator, final String fileName, final String tempFilename) throws IOException {
        try (SSTableBuilder builder = new SSTableBuilder(tempFilename)) {
            while (iterator.hasNext()) {
                builder.append(iterator.next());
            }
            builder.appendIndex();
            builder.appendTailer();
        }
        FileUtil.rename(tempFilename, fileName);
        diskStorage.addTable(fileName);
    }
}
