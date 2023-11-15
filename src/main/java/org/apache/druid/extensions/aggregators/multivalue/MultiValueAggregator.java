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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

public class MultiValueAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector;
  private final int maxStringBytes;
  private final int maxValues;
  protected Set<String> values;

  public MultiValueAggregator(BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector, int maxStringBytes, int maxValues)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.maxValues = maxValues;

    values = new HashSet<>();
  }

  @Override
  public void aggregate()
  {
    if (values.size() > maxValues) {
      return;
    }
    Object obj = valueSelector.getObject();
    String[] strValues = new String[0];
    if (obj instanceof SerializableMultiValue) {
      strValues = ((SerializableMultiValue) obj).getValues();
    } else if (obj instanceof String) {
      strValues = new String[]{(String) obj};
    }


    if (strValues != null) {
      for (String element : strValues) {
        String truncatedStr = StringUtils.fastLooseChop(element, maxStringBytes);
        values.add(truncatedStr);
        if (values.size() == maxValues) {
          break;
        }
      }
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    String[] result = values.toArray(new String[0]);
    return new SerializableMultiValue(result);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("MultiValueAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("MultiValueAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("MultiValueAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
