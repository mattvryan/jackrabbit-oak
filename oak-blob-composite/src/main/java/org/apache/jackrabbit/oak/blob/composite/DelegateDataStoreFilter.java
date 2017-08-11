/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.blob.composite;

import org.apache.jackrabbit.core.data.DataIdentifier;

import java.util.Optional;

public class DelegateDataStoreFilter {
    public static DelegateDataStoreFilter create(final String filter) {
        return new DelegateDataStoreFilter(Optional.ofNullable(filter));
    }
    public static DelegateDataStoreFilter emptyFilter() {
        return new DelegateDataStoreFilter(Optional.empty());
    }

    private DelegateDataStoreFilter(final Optional<String> filter) {

    }

    public boolean matches(final DataIdentifier identifier) {
        return false;
    }
}
