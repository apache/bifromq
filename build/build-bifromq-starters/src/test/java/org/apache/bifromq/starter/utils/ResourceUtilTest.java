/*
 * Copyright (c) 2024. The BifroMQ Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.bifromq.starter.utils;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ResourceUtilTest {

    private Path tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("testDir");
    }

    @AfterMethod
    public void tearDown() throws Exception {
        Files.walk(tempDir)
            .map(Path::toFile)
            .forEach(File::delete);
    }

    @Test
    public void absolutePathFileExists() throws Exception {
        // Create a temporary file
        Path filePath = Files.createTempFile(tempDir, "testFile", ".txt");

        // Test absolute path
        File file = ResourceUtil.loadFile(filePath.toAbsolutePath().toString());
        assertTrue(file.exists());
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void absolutePathFileDoesNotExist() throws FileNotFoundException {
        ResourceUtil.loadFile("/non/existent/path/to/file.txt");
    }

    @Test
    public void relativePathConfDirSet() throws Exception {
        // Create a temporary file in CONF_DIR
        System.setProperty("CONF_DIR", tempDir.toString());
        Path confDirFile = Files.createFile(tempDir.resolve("confFile.txt"));

        // Test relative path
        File file = ResourceUtil.loadFile("confFile.txt");
        assertTrue(file.exists());
    }

    @Test
    public void relativePathInWorkingDir() throws Exception {
        // Create a temporary file in the working directory
        Path workingDirFile = Files.createFile(tempDir.resolve("workDirFile.txt"));

        // Temporarily change the working directory to the temp directory
        System.setProperty("user.dir", tempDir.toString());

        // Test relative path
        File file = ResourceUtil.loadFile("workDirFile.txt");
        assertTrue(file.exists());
    }

    @Test
    public void testRelativePath_asResource() throws Exception {
        // Assuming there's a resource file named "testResource.txt" in src/test/resources
        File file = ResourceUtil.loadFile("testResource.txt");
        assertNotNull(file);
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void relativePathFileNotFound() throws FileNotFoundException {
        ResourceUtil.loadFile("nonexistentfile.txt");
    }
}