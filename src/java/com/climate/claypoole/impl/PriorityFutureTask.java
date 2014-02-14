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

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/** A prioritized, sortable FutureTask. */
public class PriorityFutureTask<V>
    extends FutureTask<V>
    implements Prioritized, Comparable<Prioritized> {
  private long priority;

  public PriorityFutureTask(Runnable runnable, V value, long priority) {
    super(runnable, value);
    this.priority = priority;
  }

  public PriorityFutureTask(Callable<V> callable, long priority) {
    super(callable);
    this.priority = priority;
  }

  @Override
  public int compareTo(Prioritized other) {
    // Sort for descending order.
    if (this.priority > other.getPriority()) {
      return -1;
    } else if (this.priority == other.getPriority()) {
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long getPriority() {
    return this.priority;
  }
}
