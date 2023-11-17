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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.extensions.aggregators.multivalue.BooleanOrAggregatorFactory;
import org.apache.druid.extensions.aggregators.multivalue.SerializableMultiValue;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class MultiValueHasAggregatorFactory extends AggregatorFactory
{
  public static final Comparator<Boolean> VALUE_COMPARATOR = Boolean::compare;
  private static final Aggregator NIL_AGGREGATOR = new MultiValueHasAggregator(NilColumnValueSelector.instance(), null)
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };
  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new MultiValueHasBufferAggregator(NilColumnValueSelector.instance(), null)
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };
  private final String name;
  private final String fieldName;
  private final String value;

  @JsonCreator
  public MultiValueHasAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("value") String value
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.value = value;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new MultiValueHasAggregator(
        valueSelector,
        value
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_BUFFER_AGGREGATOR;
    } else {
      return new MultiValueHasBufferAggregator(
        valueSelector,
        value
      );
    }
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    Boolean lhsBool = (Boolean) lhs;
    Boolean rhsBool = (Boolean) rhs;
    return lhsBool || rhsBool;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new MultiValueHasAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BooleanOrAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new MultiValueHasAggregatorFactory(fieldName, fieldName, value));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder((byte) 0x49)
      .appendString(fieldName)
      .appendString(value)
      .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return ColumnType.LONG;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new MultiValueHasAggregatorFactory(newName, getFieldName(), getValue());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiValueHasAggregatorFactory that = (MultiValueHasAggregatorFactory) o;
    return Objects.equals(fieldName, that.fieldName) &&
      Objects.equals(name, that.name) &&
      Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, value);
  }

  @Override
  public String toString()
  {
    return "MultiValueContainsAggregatorFactory{" +
      "fieldName='" + fieldName + '\'' +
      ", name='" + name + '\'' +
      ", value='" + value + '\'' +
      '}';
  }

}
