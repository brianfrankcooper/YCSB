/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.generator;

import java.io.File;
import java.io.IOException;

/**
 * Abstract class for generators which can record (save their results in a file) and/or recreate (reproduce the saved
 * results).
 *
 * @param <V> as would be passed to {@link Generator}.
 */
public abstract class StoringGenerator<V> extends Generator<V> {

  protected static boolean checkDataPresentAndCleanIfSomeMissing(String className, File... files) {
    if (checkIfSomeFilesAbsent(files)) {
      printIfFileMissing(className, files);
      deleteAllFiles(className, files);

      return false;
    }

    return checkAllFilesPresent(files);
  }

  private static boolean checkIfSomeFilesAbsent(File... files) {
    boolean allFilesPresent = checkAllFilesPresent(files);
    boolean allFilesAbsent = checkAllFilesAbsent(files);

    return !allFilesPresent && !allFilesAbsent;
  }

  private static boolean checkAllFilesAbsent(File... files) {
    boolean allFilesAbsent = true;

    for (File file : files) {
      allFilesAbsent = allFilesAbsent && !file.exists();
    }

    return allFilesAbsent;
  }

  private static boolean checkAllFilesPresent(File... files) {
    boolean allFilesPresent = true;

    for (File file : files) {
      allFilesPresent = allFilesPresent && file.exists();
    }

    return allFilesPresent;
  }

  private static void deleteAllFiles(String className, File... files) {
    for (File file : files) {
      if (file.delete()) {
        System.out.println(className + " deleted " + file.getName() + ".");
      }
    }
  }

  private static void printIfFileMissing(String className, File... files) {
    for (File file : files) {
      if (!file.exists()) {
        System.out.println(className + " " + file.getName() + " is missing.");
      }
    }
  }

  protected abstract String getExceptionMessage();

  protected abstract boolean checkFiles(File directory, File... files) throws IOException;
}
