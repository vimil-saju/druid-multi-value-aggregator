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

import com.google.common.collect.Iterables;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.extensions.aggregators.multivalue.MultiValueAggregatorFactory;
import org.apache.druid.extensions.aggregators.multivalue.has.MultiValueHasAggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class MultiValueHasSqlAggregator implements SqlAggregator
{
  private static final String NAME = "MV_AGG_HAS";

  private static final SqlAggFunction FUNCTION_INSTANCE = new MultiValueHasSqlAggFunction();

  static AggregatorFactory createMultiValueContainsAggregatorFactory(
      String countName,
      PlannerContext plannerContext,
      RowSignature rowSignature,
      VirtualColumnRegistry virtualColumnRegistry,
      RexBuilder rexBuilder,
      AggregateCall aggregateCall,
      Project project
  )
  {
    final RexNode rexNode = Expressions.fromFieldAccess(
        rowSignature,
        project,
        Iterables.getOnlyElement(aggregateCall.getArgList())
    );

    return new CountAggregatorFactory(countName);
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  public Aggregation toDruidAggregation(PlannerContext plannerContext, RowSignature rowSignature, VirtualColumnRegistry virtualColumnRegistry, RexBuilder rexBuilder, String name, AggregateCall aggregateCall, Project project, List<Aggregation> existingAggregations, boolean finalizeAggregations)
  {
    List<RexNode> arguments = aggregateCall
        .getArgList()
        .stream()
        .map(i -> Expressions.fromFieldAccess(rowSignature, project, i))
        .collect(Collectors.toList());

    final DruidExpression arg = Expressions.toDruidExpression(plannerContext, rowSignature, arguments.get(0));
    String fieldName;
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      VirtualColumn vc = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, arg, MultiValueAggregatorFactory.INTERMEDIATE_TYPE);
      fieldName = vc.getOutputName();
    }

    RexNode valueNode = arguments.get(1);
    if (!valueNode.isA(SqlKind.LITERAL)) {
      return null;
    }

    String value = RexLiteral.stringValue(valueNode);
    AggregatorFactory factory = new MultiValueHasAggregatorFactory(name, fieldName, value);
    return Aggregation.create(factory);
  }

  private static class MultiValueHasSqlAggFunction extends SqlAggFunction
  {
    private static final String SIGNATURE = "'" + NAME + "(column, value)'\n";

    MultiValueHasSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BOOLEAN),
          null,
          OperandTypes.and(
            OperandTypes.sequence(SIGNATURE, OperandTypes.ANY, OperandTypes.LITERAL),
            OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING)
          ),
          SqlFunctionCategory.STRING,
          false,
          false
      );
    }
  }
}
