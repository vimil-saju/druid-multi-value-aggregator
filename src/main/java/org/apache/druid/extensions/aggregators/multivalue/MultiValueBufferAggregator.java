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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MultiValueBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector;
  private final int maxStringBytes;
  private final int maxValues;
  protected Set<String> values;

  public MultiValueBufferAggregator(BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector, int maxStringBytes, int maxValues)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.maxValues = maxValues;

    values = Collections.emptySet();
  }

  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(0);
  }

  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutableBuffer = buf.duplicate();
    mutableBuffer.limit(maxValues * maxStringBytes);

    SerializableMultiValue values = (SerializableMultiValue) get(buf, position);
    Set<String> valueSet = values.getValueSet();

    Object obj = valueSelector.getObject();
    String[] strValues = new String[0];
    if (obj instanceof SerializableMultiValue) {
      strValues = ((SerializableMultiValue) obj).getValues();
    } else if (obj instanceof String) {
      strValues = new String[]{(String) obj};
    }

    for (String element : strValues) {
      String truncatedStr = StringUtils.fastLooseChop(element, maxStringBytes);
      valueSet.add(truncatedStr);
      if (valueSet.size() == maxValues) {
        break;
      }
    }

    String[] valuesArray = valueSet.toArray(new String[0]);

    int currentLen = 0;
    int count = 0;
    for (int i = 0; i < valuesArray.length; i++) {
      int prevLen = currentLen;
      String element = valuesArray[i];
      count++;
      currentLen += element.length();
      if (currentLen >= maxValues * maxStringBytes) {
        String truncatedStr = element.substring(0, maxValues * maxStringBytes - prevLen);
        valuesArray[i] = truncatedStr;
        break;
      }
    }

    mutableBuffer.putInt(count);
    for (int i = 0; i < count; i++) {
      String truncatedStr = StringUtils.fastLooseChop(valuesArray[i], maxStringBytes);
      mutableBuffer.position(mutableBuffer.position() + Integer.BYTES);
      int len = StringUtils.toUtf8WithLimit(truncatedStr, mutableBuffer);
      mutableBuffer.position(mutableBuffer.position() - Integer.BYTES);
      mutableBuffer.putInt(len);
      mutableBuffer.position(mutableBuffer.position() + len);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutableBuf = buf.duplicate();
    List<String> values = new ArrayList<>();
    int elementCount = mutableBuf.getInt(position);
    mutableBuf.position(mutableBuf.position() + Integer.BYTES);
    for (int i = 0; i < elementCount; i++) {
      values.add(StringUtils.fromUtf8(mutableBuf));
    }

    return new SerializableMultiValue(values.toArray(new String[0]));
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getDouble()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {

  }
}
