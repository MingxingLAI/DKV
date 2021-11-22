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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileUtilTest {

    private final TemporaryFolder folder = new TemporaryFolder();

    private String fileName;

    @Before
    public void setUp() {
        try {
            folder.create();
            fileName = folder.getRoot().getAbsolutePath() + File.separator + "SSTable01.sst";
            new File(fileName).createNewFile();
        } catch (IOException ioe) {
            System.err.println("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    @Test
    public void testRenameFile() throws IOException {
        String tempFilename = fileName + ".tmp";
        assertTrue(Files.exists(Paths.get(fileName)));
        assertFalse(Files.exists(Paths.get(tempFilename)));

        FileUtil.rename(fileName, tempFilename);
        assertFalse(Files.exists(Paths.get(fileName)));
        assertTrue(Files.exists(Paths.get(tempFilename)));
    }

    @After
    public void tearDown() {
        folder.delete();
    }
}
