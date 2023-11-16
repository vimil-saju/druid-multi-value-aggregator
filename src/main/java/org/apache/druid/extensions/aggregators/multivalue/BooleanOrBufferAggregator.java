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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 *
 */
public class BooleanOrBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<Boolean> selector;


  BooleanOrBufferAggregator(BaseObjectColumnValueSelector<Boolean> selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(position, 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putInt(position, Math.max(buf.getInt(position), selector.getObject() ? 1 : 0));
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getInt();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return buf.getInt();
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getInt();
  }

  @Override
  public void close()
  {

  }
}
