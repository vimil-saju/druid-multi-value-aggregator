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

package org.apache.druid.extensions.aggregators.multivalue.has;

import org.apache.druid.extensions.aggregators.multivalue.SerializableMultiValue;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

public class MultiValueHasBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector;

  private final String value;

  public MultiValueHasBufferAggregator(BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector, String value)
  {
    this.valueSelector = valueSelector;
    this.value = value;
  }

  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0);
  }

  public void aggregate(ByteBuffer buf, int position)
  {
    SerializableMultiValue object = valueSelector.getObject();
    boolean alreadyFound = buf.getLong(position) == 1;
    boolean found = alreadyFound || object.getValueSet().contains(value);
    if (found) {
      buf.putLong(position, 1);
    } else {
      buf.putLong(position, 0);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    long value = buf.getLong(position);
    return value == 1;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) get(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) get(buf, position);
  }

  @Override
  public void close()
  {

  }

}
