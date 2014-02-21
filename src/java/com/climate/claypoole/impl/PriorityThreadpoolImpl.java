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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** A fixed-size threadpool that does tasks in priority order.
 *
 * Submitted tasks have their own priority if they implement Prioritized;
 * otherwise, they are assigned the pool's default priority.
 */
public class PriorityThreadpoolImpl extends ThreadPoolExecutor {
  public PriorityThreadpoolImpl(int poolSize) {
    this(poolSize, 0);
  }

  public PriorityThreadpoolImpl(int poolSize, long defaultPriority) {
    super(poolSize, poolSize, 0, TimeUnit.MILLISECONDS,
        new PriorityBlockingQueue<Runnable>(poolSize));
    this.defaultPriority = defaultPriority;
  }

  public PriorityThreadpoolImpl(int poolSize, ThreadFactory threadFactory,
      long defaultPriority) {
    this(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, threadFactory,
        defaultPriority);
  }

  public PriorityThreadpoolImpl(int corePoolSize, int maximumPoolSize,
      long keepAliveTime, TimeUnit unit,
      ThreadFactory threadFactory, long defaultPriority) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
        new PriorityBlockingQueue<Runnable>(corePoolSize), threadFactory);
    this.defaultPriority = defaultPriority;
  }

  /** Get the priority of an object, using our defaultPriority as a backup.
   */
  protected long getPriority(Object o) {
    if (o instanceof Prioritized) {
      return ((Prioritized) o).getPriority();
    }
    return defaultPriority;
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
    return new PriorityFutureTask<T>(runnable, value, getPriority(runnable));
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    return new PriorityFutureTask<T>(callable, getPriority(callable));
  }

  public long getDefaultPriority() {
    return defaultPriority;
  }

  protected long defaultPriority;
}
