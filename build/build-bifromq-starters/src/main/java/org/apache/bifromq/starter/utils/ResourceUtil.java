/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.    
 */

package org.apache.bifromq.starter.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceUtil {
    private static final String CONF_DIR_PROP = "CONF_DIR";

    /**
     * Load file using following logic: 1. If pathToFile is absolute path, return the file if it exists. 2. If CONF_DIR
     * system property is set, try to load the file from that directory. 3. Try to load the file from the current
     * working directory. 4. Try to load the file from the classpath.
     *
     * @param pathToFile the path to the file
     * @return the file
     * @throws FileNotFoundException if the file is not found
     */
    public static File loadFile(@NonNull String pathToFile) throws FileNotFoundException {
        File file = new File(pathToFile);
        if (file.isAbsolute()) {
            if (file.exists() && file.isFile()) {
                return file;
            } else {
                throw new FileNotFoundException("File not found at absolute path: " + pathToFile);
            }
        }

        String confDir = System.getProperty(CONF_DIR_PROP);
        if (confDir != null) {
            file = new File(confDir, pathToFile);
            if (file.exists() && file.isFile()) {
                return file;
            }
        }

        String userDir = System.getProperty("user.dir");
        if (userDir != null) {
            file = new File(userDir, pathToFile);
            if (file.exists() && file.isFile()) {
                return file;
            }
        }

        URL resource = ResourceUtil.class.getClassLoader().getResource(pathToFile);
        if (resource != null) {
            return new File(resource.getFile());
        }
        throw new FileNotFoundException("File not found: " + pathToFile);
    }

    /**
     * Get file using the loadFile method. If the file is not found, return null.
     *
     * @param pathToFile the path to the file
     * @return the file or null if the file is not found
     */
    public static File getFile(@NonNull String pathToFile) {
        try {
            return loadFile(pathToFile);
        } catch (FileNotFoundException e) {
            log.debug("File not found: {}", pathToFile);
            return null;
        }
    }
}
