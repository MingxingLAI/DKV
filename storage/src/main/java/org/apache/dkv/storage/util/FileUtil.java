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

import java.io.File;
import java.io.IOException;

/**
 * file utility.
 */
public class FileUtil {

    /**
     * rename file.
     * @param sourceFileName source file
     * @param toFileName to file
     * @throws IOException IO Exception
     */
    public static void rename(final String sourceFileName, final String toFileName) throws IOException {
        File f = new File(sourceFileName);
        if (!f.renameTo(new File(toFileName))) {
            throw new IOException("Rename " + sourceFileName + " to " + toFileName + " failed when flushing");
        }
    }
}
