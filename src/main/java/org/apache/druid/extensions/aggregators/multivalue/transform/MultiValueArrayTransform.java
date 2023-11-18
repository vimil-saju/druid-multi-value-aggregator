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

package org.apache.druid.extensions.aggregators.multivalue.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Row;
import org.apache.druid.extensions.aggregators.multivalue.SerializableMultiValue;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiValueArrayTransform implements Transform
{

  private final String name;
  private final String fieldName;

  @JsonCreator
  public MultiValueArrayTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public RowFunction getRowFunction()
  {
    return new ArrayTransformFunction(fieldName);
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Collections.singleton(fieldName);
  }

  public static class ArrayTransformFunction implements RowFunction
  {
    private final String fieldName;

    public ArrayTransformFunction(String fieldName)
    {
      this.fieldName = fieldName;
    }

    @Override
    public Object eval(Row row)
    {
      return evalDimension(row);
    }

    @Override
    public List<String> evalDimension(Row row)
    {
      Object obj = row.getRaw(fieldName);
      if (obj instanceof Map) {
        return (List<String>) ((Map) obj).get("values");
      }
      if (obj instanceof SerializableMultiValue) {
        return Arrays.asList(((SerializableMultiValue) obj).getValues());
      }
      return Collections.emptyList();
    }
  }
}
