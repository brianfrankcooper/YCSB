/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
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

package site.ycsb.generator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A generator, whose sequence is the lines of a file.
 */
public class FileGenerator extends Generator<String> {
  private final String filename;
  private String current;
  private BufferedReader reader;

  /**
   * Create a FileGenerator with the given file.
   * @param filename The file to read lines from.
   */
  public FileGenerator(String filename) {
    this.filename = filename;
    reloadFile();
  }

  /**
   * Return the next string of the sequence, ie the next line of the file.
   */
  @Override
  public synchronized String nextValue() {
    try {
      current = reader.readLine();
      return current;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Return the previous read line.
   */
  @Override
  public String lastValue() {
    return current;
  }

  /**
   * Reopen the file to reuse values.
   */
  public synchronized void reloadFile() {
    try (Reader r = reader) {
      System.err.println("Reload " + filename);
      reader = new BufferedReader(new FileReader(filename));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
