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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.extensions.aggregators.multivalue.has.MultiValueHasAggregatorFactory;
import org.apache.druid.extensions.aggregators.multivalue.sql.MultiValueHasOperatorConversion;
import org.apache.druid.extensions.aggregators.multivalue.sql.MultiValueHasOptimizableFilter;
import org.apache.druid.extensions.aggregators.multivalue.sql.MultiValueHasSqlAggregator;
import org.apache.druid.extensions.aggregators.multivalue.transform.MultiValueArrayTransform;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class MultiValueAggregatorModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
      new SimpleModule("com.mulesoft.druid.extensions.aggregators.multivalue.MultiValueAggregatorModule")
        .registerSubtypes(
          new NamedType(MultiValueAggregatorFactory.class, "multiValue"),
          new NamedType(MultiValueHasAggregatorFactory.class, "multiValueAggHas"),
          new NamedType(MultiValueHasOptimizableFilter.class, "multiValueHas"),
          new NamedType(MultiValueArrayTransform.class, "multiValueArrayTransform")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    ComplexMetrics.registerSerde(SerializableMultiValueSerde.TYPE_NAME, new SerializableMultiValueSerde());
    SqlBindings.addAggregator(binder, MultiValueHasSqlAggregator.class);
    SqlBindings.addOperatorConversion(binder, MultiValueHasOperatorConversion.class);
  }
}
