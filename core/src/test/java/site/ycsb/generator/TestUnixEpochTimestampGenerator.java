/**                                                                                                                                                                                
 * Copyright (c) 2016 YCSB contributors. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package site.ycsb.generator;

import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class TestUnixEpochTimestampGenerator {

  @Test
  public void defaultCtor() throws Exception {
    final UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator();
    final long startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + 60);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + 120);
    assertEquals((long) generator.lastValue(), startTime + 60);
    assertEquals((long) generator.nextValue(), startTime + 180);
  }
  
  @Test
  public void ctorWithIntervalAndUnits() throws Exception {
    final UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator(120, TimeUnit.SECONDS);
    final long startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + 120);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + 240);
    assertEquals((long) generator.lastValue(), startTime + 120);
  }
  
  @Test
  public void ctorWithIntervalAndUnitsAndStart() throws Exception {
    final UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator(120, TimeUnit.SECONDS, 1072915200L);
    assertEquals((long) generator.nextValue(), 1072915200L);
    assertEquals((long) generator.lastValue(), 1072915200L - 120);
    assertEquals((long) generator.nextValue(), 1072915200L + 120);
    assertEquals((long) generator.lastValue(), 1072915200L);
  }
  
  @Test
  public void variousIntervalsAndUnits() throws Exception {
    // negatives could happen, just start and roll back in time
    UnixEpochTimestampGenerator generator = 
        new UnixEpochTimestampGenerator(-60, TimeUnit.SECONDS);
    long startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime - 60);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime - 120);
    assertEquals((long) generator.lastValue(), startTime - 60);
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.NANOSECONDS);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + 100);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + 200);
    assertEquals((long) generator.lastValue(), startTime + 100);
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.MICROSECONDS);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + 100);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + 200);
    assertEquals((long) generator.lastValue(), startTime + 100);
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.MILLISECONDS);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + 100);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + 200);
    assertEquals((long) generator.lastValue(), startTime + 100);
    
    generator = new UnixEpochTimestampGenerator(100, TimeUnit.SECONDS);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + 100);
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + 200);
    assertEquals((long) generator.lastValue(), startTime + 100);
    
    generator = new UnixEpochTimestampGenerator(1, TimeUnit.MINUTES);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + (1 * 60));
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + (2 * 60));
    assertEquals((long) generator.lastValue(), startTime + (1 * 60));
    
    generator = new UnixEpochTimestampGenerator(1, TimeUnit.HOURS);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + (1 * 60 * 60));
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + (2 * 60 * 60));
    assertEquals((long) generator.lastValue(), startTime + (1 * 60 * 60));
    
    generator = new UnixEpochTimestampGenerator(1, TimeUnit.DAYS);
    startTime = generator.currentValue();
    assertEquals((long) generator.nextValue(), startTime + (1 * 60 * 60 * 24));
    assertEquals((long) generator.lastValue(), startTime);
    assertEquals((long) generator.nextValue(), startTime + (2 * 60 * 60 * 24));
    assertEquals((long) generator.lastValue(), startTime + (1 * 60 * 60 * 24));
  }
  
  // TODO - With PowerMockito we could UT the initializeTimestamp(long) call.
  // Otherwise it would involve creating more functions and that would get ugly.
}
