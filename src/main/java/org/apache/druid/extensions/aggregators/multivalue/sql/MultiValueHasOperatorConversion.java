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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class MultiValueHasOperatorConversion implements SqlOperatorConversion
{
  private static final String NAME = "MV_HAS";
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(NAME)
      .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.STRING)
      .requiredOperands(2)
      .literalOperands(1)
      .returnTypeNonNull(SqlTypeName.BOOLEAN)
      .functionCategory(SqlFunctionCategory.STRING)
      .build();

  @Override
  public SqlFunction calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return OperatorConversions.convertDirectCall(
        plannerContext,
        rowSignature,
        rexNode,
        "mv_has"
    );
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final DruidExpression druidExpression = Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        operands.get(0)
    );

    if (druidExpression == null) {
      return null;
    }

    final String value = RexLiteral.stringValue(operands.get(1));

    if (druidExpression.isSimpleExtraction()) {
      return new MultiValueHasOptimizableFilter(
        druidExpression.getSimpleExtraction().getColumn(),
        value,
        druidExpression.getSimpleExtraction().getExtractionFn()
      );
    } else if (virtualColumnRegistry != null) {
      String v = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          druidExpression,
          operands.get(0).getType()
      );

      return new MultiValueHasOptimizableFilter(v, value, null);
    } else {
      return null;
    }
  }
}
