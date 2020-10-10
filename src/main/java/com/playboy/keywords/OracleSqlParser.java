package com.playboy.keywords;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectJoin;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectSubqueryTableSource;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectTableReference;
import com.alibaba.druid.util.JdbcConstants;
import com.playboy.util.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class OracleSqlParser {
    private Logger log = LoggerFactory.getLogger(OracleSqlParser.class);

    private String keyWords;

    public OracleSqlParser(String keyWords) {
        this.keyWords = keyWords;
    }


    public String dealSql(String sql) {

        String oracle = JdbcConstants.ORACLE;
        //1.解析
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, oracle);

        //2.处理
        sqlStatements.forEach(sqlStatement -> {
            if (sqlStatement instanceof SQLSelectStatement) {
                SQLSelectStatement selectStatement = (SQLSelectStatement) sqlStatement;
                SQLSelect select = selectStatement.getSelect();
                dealSelect(select);
            } else if (sqlStatement instanceof SQLUpdateStatement) {
                SQLUpdateStatement updateStatement = (SQLUpdateStatement) sqlStatement;
                dealUpdate(updateStatement);
            } else if (sqlStatement instanceof SQLInsertStatement) {
                SQLInsertStatement insertStatement = (SQLInsertStatement) sqlStatement;
                dealInsert(insertStatement);
            }
        });
        //3.组装
        String s = SQLUtils.toSQLString(sqlStatements, oracle);
        return s;
    }


    public void dealInsert(SQLInsertStatement insert) {
        if (ObjectUtils.isEmpty(insert)) {
            return;
        }
        SQLExprTableSource tableSource = insert.getTableSource();
        dealTableSource(tableSource);
        List<SQLExpr> columns = insert.getColumns();
        columns.forEach(column -> {
            dealExpr(column);
        });
        List<SQLExpr> values = insert.getValues().getValues();
        values.forEach(value -> {
            dealExpr(value);
        });
        SQLSelect query = insert.getQuery();
        dealSelect(query);
    }

    public void dealUpdate(SQLUpdateStatement update) {
        if (ObjectUtils.isEmpty(update)) {
            return;
        }
        SQLTableSource tableSource = update.getTableSource();
        dealTableSource(tableSource);

        List<SQLUpdateSetItem> items = update.getItems();
        items.forEach(sqlUpdateSetItem -> {
            SQLExpr column = sqlUpdateSetItem.getColumn();
            dealExpr(column);
            SQLExpr value = sqlUpdateSetItem.getValue();
            dealExpr(value);
        });

        SQLExpr where = update.getWhere();
        dealExpr(where);
    }


    public void dealSelect(SQLSelect select) {
        if (ObjectUtils.isEmpty(select)) {
            return;
        }
        SQLSelectQuery query = select.getQuery();
        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
            List<SQLSelectItem> selectList = queryBlock.getSelectList();
            selectList.forEach(sqlSelectItem -> {
                String alias = sqlSelectItem.getAlias();
                if (alias != null) {
                    sqlSelectItem.setAlias(dealName(alias));
                }
                SQLExpr expr = sqlSelectItem.getExpr();
                dealExpr(expr);
            });
            SQLTableSource from = queryBlock.getFrom();
            dealTableSource(from);
            SQLExpr where = queryBlock.getWhere();
            dealExpr(where);
        } else if (query instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) query;
            SQLSelectQuery left = sqlUnionQuery.getLeft();
            SQLSelectQuery right = sqlUnionQuery.getRight();
        }
    }

    /**
     * @param tableSource 处理table表来源
     */
    public void dealTableSource(SQLTableSource tableSource) {
        if (ObjectUtils.isEmpty(tableSource)) {
            return;
        }
        if (tableSource instanceof OracleSelectJoin) {//联查
            OracleSelectJoin oracleSelectJoin = (OracleSelectJoin) tableSource;
            SQLTableSource left = oracleSelectJoin.getLeft();
            dealTableSource(left);
            SQLTableSource right = oracleSelectJoin.getRight();
            dealTableSource(right);
            SQLExpr condition = oracleSelectJoin.getCondition(); //TODO
            dealExpr(condition);
        } else if (tableSource instanceof OracleSelectSubqueryTableSource) { //子表
            OracleSelectSubqueryTableSource oracleSelectSubqueryTableSource = (OracleSelectSubqueryTableSource) tableSource;
            SQLSelect select = oracleSelectSubqueryTableSource.getSelect();
            dealSelect(select);
        } else if (tableSource instanceof OracleSelectTableReference) { //正式数据库表
            OracleSelectTableReference oracleSelectTableReference = (OracleSelectTableReference) tableSource;
        }
    }

    public void dealExpr(SQLExpr expr) {
        if (ObjectUtils.isEmpty(expr)) {
            return;
        }
        if (expr instanceof SQLIdentifierExpr) {//BILL_ID
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) expr;
            String name = sqlIdentifierExpr.getName();
            sqlIdentifierExpr.setName(dealName(name));
        } else if (expr instanceof SQLAggregateExpr) {//COUNT(DISTINCT b.BILL_ID)
            SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) expr;
            if (sqlAggregateExpr.getMethodName().equalsIgnoreCase("ROW_NUMBER")) {
                SQLOver over = sqlAggregateExpr.getOver();
                List<SQLExpr> partitionBys = over.getPartitionBy();
                partitionBys.forEach(partitionBy -> {
                    dealExpr(partitionBy);
                });
            } else {
                List<SQLExpr> arguments = sqlAggregateExpr.getArguments();
                arguments.forEach(argument -> {
                    dealExpr(argument);
                });
            }
        } else if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) expr;
            String name = sqlPropertyExpr.getName();
            if (!name.equals("*")) {
                sqlPropertyExpr.setName(dealName(name));
            }
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> parameters = sqlMethodInvokeExpr.getParameters();
            parameters.forEach(parameter -> {
                dealExpr(parameter);
            });
        } else if (expr instanceof SQLQueryExpr) {
            SQLQueryExpr sqlQueryExpr = (SQLQueryExpr) expr;
            SQLSelect subQuery = sqlQueryExpr.getSubQuery();
            dealSelect(subQuery);
        } else if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            SQLExpr left = sqlBinaryOpExpr.getLeft();
            dealExpr(left);
            SQLExpr right = sqlBinaryOpExpr.getRight();
            dealExpr(right);
        } else if (expr instanceof SQLBetweenExpr) {
            SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) expr;
            SQLExpr testExpr = sqlBetweenExpr.getTestExpr();
            dealExpr(testExpr);
        }
    }

    public String dealName(String name) {
        if (!ObjectUtils.isEmpty(name)) {
            if (keyWords.contains(name)) {
                return "\"" + name + "\"";
            }
        }
        return name;
    }
}
