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

package org.apache.druid.extensions.aggregators.multivalue;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

public class MultiValueUtils
{

  public static SerializableMultiValue combine(@Nullable Object lhs, @Nullable Object rhs, int maxValues)
  {
    SerializableMultiValue lhsMultiValue = (SerializableMultiValue) lhs;
    SerializableMultiValue rhsMultiValue = (SerializableMultiValue) rhs;

    Set<String> lhsSet = Collections.emptySet();
    Set<String> rhsSet = Collections.emptySet();
    if (lhsMultiValue != null) {
      lhsSet = lhsMultiValue.getValueSet();
    }

    if (rhsMultiValue != null) {
      rhsSet = rhsMultiValue.getValueSet();
    }

    Set<String> set = new TreeSet<>();
    set.addAll(lhsSet);
    set.addAll(rhsSet);
    String[] result = new ArrayList<>(set).subList(0, Math.min(set.size(), maxValues)).toArray(new String[0]);
    return new SerializableMultiValue(result);
  }
}
