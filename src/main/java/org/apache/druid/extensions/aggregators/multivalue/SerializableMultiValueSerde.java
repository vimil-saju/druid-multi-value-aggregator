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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class SerializableMultiValueSerde extends ComplexMetricSerde
{
  protected static final String TYPE_NAME = "serializableMultiValue";

  private static final Comparator<SerializableMultiValue> COMPARATOR = Comparator.nullsFirst(
      Comparator.comparing(SerializableMultiValue::getValueSet, new Comparator<Set<String>>()
      {
        @Override
        public int compare(Set<String> o1, Set<String> o2)
        {
          if (o1 == o2) {
            return 0;
          }

          if (o1 == null) {
            return -1;
          }
          if (o2 == null) {
            return 1;
          }

          return o1.size() - o2.size();
        }
      })
  );

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<SerializableMultiValue> extractedClass()
      {
        return SerializableMultiValue.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        return inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<SerializableMultiValue>()
    {
      @Override
      public int compare(@Nullable SerializableMultiValue o1, @Nullable SerializableMultiValue o2)
      {
        return COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializableMultiValue> getClazz()
      {
        return SerializableMultiValue.class;
      }

      @Override
      public SerializableMultiValue fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

        int stringCount = readOnlyBuffer.getInt();

        List<String> values = new ArrayList<>();

        for (int i = 0; i < stringCount; i++) {
          int stringSize = readOnlyBuffer.getInt();
          byte[] stringBytes = new byte[stringSize];
          readOnlyBuffer.get(stringBytes, 0, stringSize);
          String str = StringUtils.fromUtf8(stringBytes);
          values.add(str);
        }

        return new SerializableMultiValue(values.toArray(new String[0]));
      }

      @Override
      public byte[] toBytes(SerializableMultiValue val)
      {
        String[] strValues = val.getValues();
        ByteBuffer bbuf;

        if (strValues != null) {
          int bufferSize = Integer.BYTES;
          for (int i = 0; i < strValues.length; i++) {
            bufferSize += Integer.BYTES;
            String strValue = strValues[i];
            byte[] strBytes = StringUtils.toUtf8(strValue);
            bufferSize += strBytes.length;
          }

          bbuf = ByteBuffer.allocate(bufferSize);

          bbuf.putInt(strValues.length);

          for (int i = 0; i < strValues.length; i++) {
            String strValue = strValues[i];
            byte[] strBytes = StringUtils.toUtf8(strValue);

            bbuf.putInt(strBytes.length);
            bbuf.put(strBytes);
          }
        } else {
          bbuf = ByteBuffer.allocate(Integer.BYTES);
          bbuf.putInt(0);
        }

        return bbuf.array();
      }
    };
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
