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
package site.ycsb;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test class for {@link Status}.
 */
public class TestStatus {

  @Test
  public void testAcceptableStatus() {
    assertTrue(Status.OK.isOk());
    assertTrue(Status.BATCHED_OK.isOk());
    assertFalse(Status.BAD_REQUEST.isOk());
    assertFalse(Status.ERROR.isOk());
    assertFalse(Status.FORBIDDEN.isOk());
    assertFalse(Status.NOT_FOUND.isOk());
    assertFalse(Status.NOT_IMPLEMENTED.isOk());
    assertFalse(Status.SERVICE_UNAVAILABLE.isOk());
    assertFalse(Status.UNEXPECTED_STATE.isOk());
  }
}
