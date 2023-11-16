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

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

public class MultiValueContainsAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector;
  private final String value;

  private boolean found;

  public MultiValueContainsAggregator(BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector, String value)
  {
    this.valueSelector = valueSelector;
    this.value = value;
  }

  @Override
  public void aggregate()
  {
    found = found || valueSelector.getObject().getValueSet().contains(value);
  }

  @Override
  public Object get()
  {
    return found ? 1 : 0;
  }

  @Override
  public float getFloat()
  {
    return found ? 1 : 0;
  }

  @Override
  public long getLong()
  {
    return found ? 1 : 0;
  }

  @Override
  public double getDouble()
  {
    return found ? 1 : 0;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
