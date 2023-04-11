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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.common.Config;
import com.starrocks.connector.parser.trino.TrinoParserUtils;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.StatementBase;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.StatementSplitter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class SqlParser {
    private static final Logger LOG = LogManager.getLogger(SqlParser.class);

    public static List<StatementBase> parse(String sql, SessionVariable sessionVariable) {
        if (sessionVariable.getSqlDialect().equalsIgnoreCase("trino")) {
            return parseWithTrinoDialect(sql, sessionVariable);
        } else {
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
    }

    private static List<StatementBase> parseWithTrinoDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        try {
            StatementSplitter splitter = new StatementSplitter(sql);
            for (StatementSplitter.Statement statement : splitter.getCompleteStatements()) {
                statements.add(TrinoParserUtils.toStatement(statement.statement(), sessionVariable.getSqlMode()));
            }
            if (!splitter.getPartialStatement().isEmpty()) {
                statements.add(TrinoParserUtils.toStatement(splitter.getPartialStatement(),
                        sessionVariable.getSqlMode()));
            }
        } catch (ParsingException e) {
            // we only support trino partial syntax, use StarRocks parser to parse now
            LOG.warn("Trino parse sql [{}] error, cause by {}", sql, e);
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
        if (statements.isEmpty() || statements.stream().anyMatch(Objects::isNull)) {
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
        return statements;
    }

    private static List<StatementBase> parseWithStarRocksDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        StarRocksParser parser = parserBuilder(sql, sessionVariable);
        List<StarRocksParser.SingleStatementContext> singleStatementContexts =
                parser.sqlStatements().singleStatement();
        for (int idx = 0; idx < singleStatementContexts.size(); ++idx) {
            StatementBase statement = (StatementBase) new AstBuilder(sessionVariable.getSqlMode())
                    .visitSingleStatement(singleStatementContexts.get(idx));
            statement.setOrigStmt(new OriginStatement(sql, idx));
            statements.add(statement);
        }
        return statements;
    }

    /**
     * We need not only sqlMode but also other parameters to define the property of parser.
     * Please consider use {@link #parse(String, SessionVariable)}
     */
    @Deprecated
    public static List<StatementBase> parse(String originSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);
        return parse(originSql, sessionVariable);
    }

    public static StatementBase parseFirstStatement(String originSql, long sqlMode) {
        return parse(originSql, sqlMode).get(0);
    }

    /**
     * parse sql to expression, only supports new parser
     *
     * @param expressionSql expression sql
     * @param sqlMode       sqlMode
     * @return Expr
     */
    public static Expr parseSqlToExpr(String expressionSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);

        return (Expr) new AstBuilder(sqlMode)
                .visit(parserBuilder(expressionSql, sessionVariable).expressionSingleton().expression());
    }

    public static ImportColumnsStmt parseImportColumns(String expressionSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);

        return (ImportColumnsStmt) new AstBuilder(sqlMode)
                .visit(parserBuilder(expressionSql, sessionVariable).importColumns());
    }

    private static StarRocksParser parserBuilder(String sql, SessionVariable sessionVariable) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(sessionVariable.getSqlMode());
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);

        // Unify the error message
        parser.setErrorHandler(new DefaultErrorStrategy() {
            @Override
            public Token recoverInline(Parser recognizer)
                    throws RecognitionException {
                if (nextTokensContext == null) {
                    throw new InputMismatchException(recognizer);
                } else {
                    throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                }
            }

            @Override
            protected void reportMissingToken(Parser recognizer) {
                reportMissMatchedToken(recognizer);
            }

            @Override
            protected void reportUnwantedToken(Parser recognizer) {
                reportMissMatchedToken(recognizer);
            }

            private void reportMissMatchedToken(Parser recognizer) {
                if (inErrorRecoveryMode(recognizer)) {
                    return;
                }

                beginErrorCondition(recognizer);
                Token t = recognizer.getCurrentToken();
                String tokenName = getTokenDisplay(t);
                recognizer.notifyErrorListeners(t, PARSER_ERROR_MSG.inputMismatch(tokenName), null);
            }
        });

        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(
                new PostProcessListener(sessionVariable.getParseTokensLimit(), Config.expr_children_limit));
        return parser;
    }

    public static String getTokenDisplay(Token t) {
        if (t == null) {
            return "<no token>";
        }

        String s = t.getText();
        if (s == null) {
            if (t.getType() == Token.EOF) {
                s = "<EOF>";
            } else {
                s = "<" + t.getType() + ">";
            }
        }
        return s;
    }
}