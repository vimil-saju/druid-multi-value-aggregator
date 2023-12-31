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

import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class BooleanOrAggregateCombiner implements AggregateCombiner<Boolean>
{
  private boolean value;

  @Override
  public void reset(ColumnValueSelector selector)
  {
    value = selector.getLong() == 1;
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    value = value || (selector.getLong() == 1);
  }

  @Override
  public double getDouble()
  {
    return value ? 1 : 0;
  }

  @Override
  public float getFloat()
  {
    return value ? 1 : 0;
  }

  @Override
  public long getLong()
  {
    return value ? 1 : 0;
  }

  @Nullable
  @Override
  public Boolean getObject()
  {
    return value;
  }

  @Override
  public Class<? extends Boolean> classOfObject()
  {
    return Boolean.class;
  }
}
