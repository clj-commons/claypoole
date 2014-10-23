// The Climate Corporation licenses this file to you under under the Apache
// License, Version 2.0 (the "License"); you may not use this file except in
// compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// See the NOTICE file distributed with this work for additional information
// regarding copyright ownership.  Unless required by applicable law or agreed
// to in writing, software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions
// and limitations under the License.

package com.climate.claypoole.impl;

import clojure.lang.IFn;

/** A class that calls a function when it is finalized.
 *
 * This is helpful for testing against memory leaks, and we do it in Java
 * because gen-class is so much work.
 */
public class TestFinalize extends Object {
  private IFn finalizeFn;

  public TestFinalize(IFn finalizeFn) {
    this.finalizeFn = finalizeFn;
  }

  protected void finalize() {
    this.finalizeFn.invoke();
  }
}
