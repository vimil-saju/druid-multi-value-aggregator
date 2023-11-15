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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("multiValue")
public class MultiValueAggregatorFactory extends AggregatorFactory
{
  public static final ColumnType INTERMEDIATE_TYPE = ColumnType.ofComplex(SerializableMultiValueSerde.TYPE_NAME);
  public static final Comparator<String[]> VALUE_COMPARATOR =
      new Comparator<String[]>()
      {
        @Override
        public int compare(String[] a, String[] b)
        {
          if (a == b) {
            return 0;
          }

          // A null array is less than a non-null array
          if (a == null || b == null) {
            return a == null ? -1 : 1;
          }

          int length = Math.min(a.length, b.length);
          for (int i = 0; i < length; i++) {
            String oa = a[i];
            String ob = b[i];
            if (oa != ob) {
              // A null element is less than a non-null element
              if (oa == null || ob == null) {
                return oa == null ? -1 : 1;
              }
              int v = oa.compareTo(ob);
              if (v != 0) {
                return v;
              }
            }
          }

          return a.length - b.length;
        }
      };
  private static final Aggregator NIL_AGGREGATOR = new MultiValueAggregator(NilColumnValueSelector.instance(), 0, 0)
  {
    @Override
    public void aggregate()
    {
      // no-op
    }
  };
  private static final BufferAggregator NIL_BUFFER_AGGREGATOR = new MultiValueBufferAggregator(NilColumnValueSelector.instance(), 0, 0)
  {
    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }
  };
  protected final int maxStringBytes;
  protected final int maxValues;
  private final String fieldName;
  private final String name;

  @JsonCreator
  public MultiValueAggregatorFactory(@JsonProperty("name") String name, @JsonProperty("fieldName") final String fieldName, @JsonProperty("maxStringBytes") Integer maxStringBytes, @JsonProperty("maxValues") Integer maxValues)
  {
    this.name = name;
    this.fieldName = fieldName;
    this.maxStringBytes = maxStringBytes == null
      ? 1024
      : maxStringBytes;

    this.maxValues = maxValues == null
      ? 1024
      : maxValues;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final BaseObjectColumnValueSelector<SerializableMultiValue> valueSelector = metricFactory.makeColumnValueSelector(fieldName);
    if (valueSelector instanceof NilColumnValueSelector) {
      return NIL_AGGREGATOR;
    } else {
      return new MultiValueAggregator(
        valueSelector,
        maxStringBytes,
        maxValues
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
      return new MultiValueBufferAggregator(
        valueSelector,
        maxStringBytes,
        maxValues
      );
    }
  }

  @Override
  public Comparator getComparator()
  {
    return VALUE_COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    SerializableMultiValue lhsStr = (SerializableMultiValue) lhs;
    SerializableMultiValue rhsStr = (SerializableMultiValue) rhs;
    return MultiValueUtils.combine(lhsStr, rhsStr, maxValues);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new MultiValueAggregateCombiner(maxValues);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new MultiValueAggregatorFactory(name, name, maxStringBytes, maxValues);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new MultiValueAggregatorFactory(fieldName, fieldName, maxStringBytes, maxValues));
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
    return object == null ? null : ((SerializableMultiValue) object).getValues();
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
  public Integer getMaxValues()
  {
    return maxValues;
  }

  @JsonProperty
  public Integer getMaxStringBytes()
  {
    return maxStringBytes;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES + (Integer.BYTES + maxStringBytes) * maxValues;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder((byte) 0x48)
      .appendString(fieldName)
      .appendInt(maxStringBytes)
      .appendInt(maxValues)
      .build();
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return INTERMEDIATE_TYPE;
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.STRING_ARRAY;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new MultiValueAggregatorFactory(newName, getFieldName(), getMaxStringBytes(), getMaxValues());
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
    MultiValueAggregatorFactory that = (MultiValueAggregatorFactory) o;
    return maxStringBytes == that.maxStringBytes &&
      maxValues == that.maxValues &&
      Objects.equals(fieldName, that.fieldName) &&
      Objects.equals(name, that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name, maxStringBytes, maxValues);
  }

  @Override
  public String toString()
  {
    return "MultiValueAggregatorFactory{" +
      "fieldName='" + fieldName + '\'' +
      ", name='" + name + '\'' +
      ", maxStringBytes=" + maxStringBytes +
      ", maxValues=" + maxValues +
      '}';
  }
}
