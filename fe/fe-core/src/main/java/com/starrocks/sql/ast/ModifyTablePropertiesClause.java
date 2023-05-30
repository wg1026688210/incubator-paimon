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


package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import javax.annotation.Nonnull;

// clause which is used to modify table properties
public class ModifyTablePropertiesClause extends AlterTableClause {
    private final Map<String, String> properties;

    public ModifyTablePropertiesClause(Map<String, String> properties) {
        this(properties, NodePosition.ZERO);
    }

    public ModifyTablePropertiesClause(Map<String, String> properties, NodePosition pos) {
        super(AlterOpType.MODIFY_TABLE_PROPERTY, pos);
        this.properties = properties;
    }

    public void setOpType(AlterOpType opType) {
        this.opType = opType;
    }

    public void setNeedTableStable(boolean needTableStable) {
        this.needTableStable = needTableStable;
    }

    @Override
    @Nonnull
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitModifyTablePropertiesClause(this, context);
    }
}