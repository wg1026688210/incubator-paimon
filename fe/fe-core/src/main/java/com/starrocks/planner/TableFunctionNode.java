// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.TableFunction;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalTableFunctionNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TTableFunctionNode;
import com.starrocks.thrift.TTypeDesc;

import java.util.List;

public class TableFunctionNode extends PlanNode {
    private final TableFunction tableFunction;
    //Slots of output by table function
    private final List<Integer> fnResultSlots;
    //External column slots of the join logic generated by the table function
    private final List<Integer> outerSlots;
    //Slots of table function input parameters
    private final List<Integer> paramSlots;

    public TableFunctionNode(PlanNodeId id, PlanNode child, TupleDescriptor outputTupleDesc,
                             TableFunction tableFunction,
                             List<Integer> paramSlots,
                             List<Integer> outerSlots,
                             List<Integer> fnResultSlots) {
        super(id, "TableValueFunction");
        this.children.add(child);
        this.tableFunction = tableFunction;

        this.paramSlots = paramSlots;
        this.outerSlots = outerSlots;
        this.fnResultSlots = fnResultSlots;
        this.tupleIds.add(outputTupleDesc.getId());
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.TABLE_FUNCTION_NODE;
        msg.table_function_node = new TTableFunctionNode();

        TExprNode tExprNode = new TExprNode(TExprNodeType.TABLE_FUNCTION_EXPR,
                new TTypeDesc(), tableFunction.getNumArgs(), 0);
        tExprNode.setFn(tableFunction.toThrift());

        TExpr texpr = new TExpr();
        texpr.addToNodes(tExprNode);

        msg.table_function_node.setTable_function(texpr);
        msg.table_function_node.setParam_columns(paramSlots);
        msg.table_function_node.setOuter_columns(outerSlots);
        msg.table_function_node.setFn_result_columns(fnResultSlots);
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("tableFunctionName: ").append(tableFunction.getFunctionName()).append('\n');
        output.append(prefix).append("columns: ").append(tableFunction.getDefaultColumnNames()).append('\n');
        output.append(prefix).append("returnTypes: ").append(tableFunction.getTableFnReturnTypes()).append('\n');
        return output.toString();
    }

    @Override
    public boolean canUsePipeLine() {
        return getChildren().stream().allMatch(PlanNode::canUsePipeLine);
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return getChildren().stream().allMatch(PlanNode::canUseRuntimeAdaptiveDop);
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalTableFunctionNode tableFunctionNode = new TNormalTableFunctionNode();
        tableFunctionNode.setTable_function(tableFunction.toThrift());
        tableFunctionNode.setParam_columns(normalizer.remapIntegerSlotIds(paramSlots));
        tableFunctionNode.setOuter_columns(normalizer.remapIntegerSlotIds(outerSlots));
        tableFunctionNode.setFn_result_columns(normalizer.remapIntegerSlotIds(fnResultSlots));
        planNode.setTable_function_node(tableFunctionNode);
        planNode.setNode_type(TPlanNodeType.TABLE_FUNCTION_NODE);
        normalizeConjuncts(normalizer, planNode, conjuncts);
    }
}