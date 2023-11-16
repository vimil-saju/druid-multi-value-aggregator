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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.AbstractOptimizableDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

public class MultiValueHasOptimizableFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final String dimension;
  private final String value;
  @Nullable
  private final ExtractionFn extractionFn;

  @JsonCreator
  public MultiValueHasOptimizableFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn
  )
  {
    this.dimension = dimension;
    this.value = value;
    this.extractionFn = extractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Nullable
  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }


  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] valueBytes = StringUtils.toUtf8(value);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + valueBytes.length)
      .put((byte) 0x55)
      .put(dimensionBytes)
      .put((byte) 0xFF)
      .put(valueBytes)
      .put((byte) 0xFF)
      .put(extractionFnBytes)
      .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new MultiValueHasFilter(dimension, value, extractionFn);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public String toString()
  {
    return "MultiValueHasOptimizableFilter{" +
      "dimension='" + dimension + '\'' +
      ", value='" + value + '\'' +
      ", extractionFn='" + extractionFn + '\'' +
      '}';
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
    MultiValueHasOptimizableFilter that = (MultiValueHasOptimizableFilter) o;
    return dimension.equals(that.dimension) &&
      value.equals(that.value) &&
      Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, value, extractionFn);
  }
}
