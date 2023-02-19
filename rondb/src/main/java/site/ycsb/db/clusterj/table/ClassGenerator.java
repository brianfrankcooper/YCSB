/*
 * Copyright (c) 2021, Yahoo!, Inc. All rights reserved.
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

/**
 * YCSB binding for <a href="https://rondb.com/">RonDB</a>.
 */
package site.ycsb.db.clusterj.table;

import javassist.*;

import java.io.Serializable;

/**
 * Generated classes extending MySQL NDB Dynamic Objects.
 */
public class ClassGenerator implements Serializable {

  private ClassPool pool;
  private static Class<?> tableObj = null;

  public ClassGenerator() {
    pool = ClassPool.getDefault();
  }

  public Class<?> generateClass(String tableName) throws NotFoundException, CannotCompileException {
    if (tableObj != null) {
      return tableObj;
    }

    synchronized (this) {
      if (tableObj == null) {
        CtClass originalClass = pool.get("site.ycsb.db.clusterj.table.DBTable");
        originalClass.defrost();

        String methodCode = "public String table() { return \"" + tableName + "\"; }";
        CtMethod tableMethod = CtMethod.make(methodCode, originalClass);
        originalClass.addMethod(tableMethod);

        tableObj = originalClass.toClass();
      }
      return tableObj;
    }
  }
}
