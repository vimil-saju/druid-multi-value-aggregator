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

package org.apache.druid.extensions.aggregators.multivalue.sql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import org.apache.druid.extensions.aggregators.multivalue.SerializableMultiValue;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.FilterTuning;
import org.apache.druid.segment.filter.DimensionPredicateFilter;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class MultiValueHasFilter extends DimensionPredicateFilter
{
  private final String value;

  public MultiValueHasFilter(String dimension, String value, ExtractionFn extractionFn)
  {
    super(dimension, new MultiValuePredicateFactory(value), extractionFn, new FilterTuning(false, null, null));
    this.value = value;
  }

  @Override
  public String toString()
  {
    return "MultiValueContainsFilter{" +
      "value='" + value + '\'' +
      '}';
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(dimension);

    if (rewriteDimensionTo == null) {
      throw new IAE(
        "Received a non-applicable rewrite: %s, filter's dimension: %s",
        columnRewrites,
        dimension
      );
    }

    return new MultiValueHasFilter(
      rewriteDimensionTo,
      value,
      extractionFn
    );
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
    if (!super.equals(o)) {
      return false;
    }
    MultiValueHasFilter that = (MultiValueHasFilter) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), value);
  }

  @VisibleForTesting
  static class MultiValuePredicateFactory implements DruidPredicateFactory
  {
    private final String value;

    MultiValuePredicateFactory(String value)
    {
      this.value = value;
    }

    @Override
    public Predicate<String> makeStringPredicate()
    {
      throw new UnsupportedOperationException("MultiValueContains Filter does not support String comparison");
    }

    @Override
    public DruidLongPredicate makeLongPredicate()
    {
      throw new UnsupportedOperationException("MultiValueContains Filter does not support String comparison");
    }

    @Override
    public DruidFloatPredicate makeFloatPredicate()
    {
      throw new UnsupportedOperationException("MultiValueContains Filter does not support String comparison");
    }

    @Override
    public DruidDoublePredicate makeDoublePredicate()
    {
      throw new UnsupportedOperationException("MultiValueContains Filter does not support String comparison");
    }

    @Override
    public Predicate<Object> makeObjectPredicate()
    {
      return new Predicate<Object>()
      {
        @Override
        public boolean apply(@Nullable Object input)
        {
          SerializableMultiValue multiValueInput = (SerializableMultiValue) input;
          return multiValueInput.getValueSet().contains(value);
        }
      };
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
      MultiValuePredicateFactory that = (MultiValuePredicateFactory) o;
      return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(value);
    }

    @Override
    public String toString()
    {
      return "MultiValueContainsFilter$MultiValuePredicateFactory{" +
        "value='" + value + '\'' +
        '}';
    }
  }
}
