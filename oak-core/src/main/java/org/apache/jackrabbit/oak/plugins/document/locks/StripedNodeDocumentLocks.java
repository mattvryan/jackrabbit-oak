/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.locks;

import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;

public class StripedNodeDocumentLocks implements NodeDocumentLocks {

    /**
     * Locks to ensure cache consistency on reads, writes and invalidation.
     */
    private final Striped<Lock> locks = Striped.lock(4096);
    private final Lock rootLock = Striped.lock(1).get("0:/");

    @Override
    public Lock acquire(String key) {
        Lock lock = "0:/".equals(key) ? rootLock : locks.get(key);
        lock.lock();
        return lock;
    }
}
