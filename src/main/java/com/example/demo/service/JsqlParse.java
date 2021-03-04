package com.demo.it.jsqlparse.parse;

import com.demo.it.jsqlparse.bean.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.springframework.stereotype.Component;

import java.io.StringReader;
import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@Component
public class ParseSelectService {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private SqlBean sqlBean;
    private String sql;

    /**
     * SQL解析
     */
    public SqlBean parse(String sql) {
        this.sql = sql;
        sqlBean = new SqlBean();
        try {
            parseSelectTable()
                    .parseSelectColumn()
                    .parseSelectJoin()
                    .parseSelectWhere()
                    .parseSelectGroupby()
                    .parseSelectOrderby()
                    .parseSelectLimit();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sqlBean;
    }

    /**
     * 解析查询字段
     */
    public ParseSelectService parseSelectColumn() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Select select = (Select) parserManager.parse(new StringReader(sql));
        PlainSelect plain = (PlainSelect) select.getSelectBody();
        List<SelectItem> selectItemList = plain.getSelectItems();
        if (selectItemList == null || selectItemList.size() == 0) {
            return this;
        }
        List<ColumnBean> columnBeanList = new ArrayList<>();
        selectItemList.forEach(item -> {
            SelectExpressionItem expressionItem = (SelectExpressionItem) item;
            //表别名
            String tableAlias = "";
            //列名
            String columnName = "";
            //列别名
            String columnAlias = "";
            //函数名
            String functionName = "";
            if (expressionItem.getAlias() != null) {
                columnAlias = expressionItem.getAlias().getName();
            }
            Expression expression = expressionItem.getExpression();
            //普通字段
            if (expression instanceof Column) {
                Column column = (Column) expression;
                columnName = column.getColumnName();
                if (column.getTable() != null) {
                    tableAlias = column.getTable().getName();
                }
            }
            //函数字段
            else if (expression instanceof Function) {
                Function function = (Function) expression;
                //函数名
                functionName = function.getName();
                Expression expreItem = function.getParameters().getExpressions().get(0);
                if (expreItem instanceof Column) {
                    Column column = (Column) expreItem;
                    columnName = column.getColumnName();
                    if (column.getTable() != null) {
                        tableAlias = column.getTable().getName();
                    }
                }
            }
            //子查询
            else if (expression instanceof SubSelect) {
                SubSelect subSelect = (SubSelect) expression;
                columnName = subSelect.toString();
                //去掉子查询两边的()
                // columnName = subSelect.getSelectBody().toString();
            }
            ColumnBean columnBean = new ColumnBean();
            columnBean.setColumnName(columnName);
            columnBean.setColumnAlis(columnAlias);
            columnBean.setFunctionName(functionName);
            columnBean.setTableName(null);
            columnBean.setTableAlias(tableAlias);
            columnBeanList.add(columnBean);
        });

        sqlBean.setColumnBeanList(columnBeanList);
        return this;
    }

    /**
     * 获取表信息
     */
    public ParseSelectService parseSelectTable() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        List<TableBean> tableBeanList = new ArrayList<>();
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select select = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        //表名称
        String tableName = null;
        //表别名
        String tableAlias = null;
        //获取主表
        FromItem fromItem = plainSelect.getFromItem();
        if (fromItem instanceof Table) {
            Table table = (Table) fromItem;
            tableName = table.getName();
            if (table.getAlias() != null) {
                tableAlias = table.getAlias().getName();
            }
        }
        tableBeanList.add(new TableBean(tableName, tableAlias));
        //获取连接表
        List<Join> joins = plainSelect.getJoins();
        if (joins != null && joins.size() > 0) {
            for (Join join : joins) {
                FromItem rightItem = join.getRightItem();
                if (rightItem instanceof Table) {
                    Table table = (Table) rightItem;
                    tableName = table.getName();
                    if (table.getAlias() != null) {
                        tableAlias = table.getAlias().getName();
                    }
                    tableBeanList.add(new TableBean(tableName, tableAlias));
                }
            }
        }
        sqlBean.setTableBeanList(tableBeanList);
        return this;
    }

    /**
     * 获取JOIN信息
     */
    public ParseSelectService parseSelectJoin() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        List<JoinBean> joinBeanList = new ArrayList<>();
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
        List<Join> joins = plain.getJoins();
        if (joins == null || joins.size() == 0) {
            return this;
        }
        for (Join join : joins) {
            JoinBean joinBean = new JoinBean();
            TableBean joinTable = null;
            FromItem rightItem = join.getRightItem();
            //连接表信息
            if (rightItem instanceof Table) {
                Table table = (Table) rightItem;
                joinTable = new TableBean();
                joinTable.setTableName(table.getName());
                if (table.getAlias() != null) {
                    joinTable.setTableAlias(table.getAlias().getName());
                }
            }
            //on,and,or
            Expression onExpression = join.getOnExpression();
            List<JoinOn> joinOnList = parseExpression(onExpression, "ON");
            //
            joinBean.setJoinType(getJoinType(join));
            joinBean.setTableBean(joinTable);
            joinBean.setJoinOnList(joinOnList);
            joinBeanList.add(joinBean);
        }
        sqlBean.setJoinBeanList(joinBeanList);
        return this;
    }

    /**
     * 获取WHERE信息
     */
    public ParseSelectService parseSelectWhere() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        WhereBean whereBean = new WhereBean();
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
        Expression where = plain.getWhere();
        List<JoinOn> joinOnList = parseExpression(where, "");
        whereBean.setJoinOnList(joinOnList);
        sqlBean.setWhereBean(whereBean);
        return this;
    }

    /**
     * 获取GROUP BY信息
     */
    public ParseSelectService parseSelectGroupby() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
        GroupByElement groupBy = plain.getGroupBy();
        List<Expression> groupByExpressions = groupBy.getGroupByExpressions();
        if (groupByExpressions == null || groupByExpressions.size() == 0) {
            return this;
        }
        GroupbyBean groupbyBean = new GroupbyBean();
        List<ColumnItem> columnItemList = new ArrayList<>();
        groupbyBean.setColumnItemList(columnItemList);
        for (Expression expression : groupByExpressions) {
            if (expression instanceof Column) {
                ColumnItem columnItem = getLeftExpressionValue(expression);
                columnItemList.add(columnItem);
            }
        }
        sqlBean.setGroupbyBean(groupbyBean);
        return this;
    }

    /**
     * 获取ORDER BY信息
     */
    public ParseSelectService parseSelectOrderby() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
        List<OrderByElement> orderByElements = plain.getOrderByElements();
        if (orderByElements == null || orderByElements.size() == 0) {
            return this;
        }
        OrderbyBean orderbyBean = new OrderbyBean();
        List<ColumnItem> columnItemList = new ArrayList<>();
        orderbyBean.setColumnItemList(columnItemList);
        for (OrderByElement element : orderByElements) {
            boolean asc = element.isAsc();
            Expression expression = element.getExpression();
            if (expression instanceof Column) {
                ColumnItem columnItem = getLeftExpressionValue(expression);
                columnItem.setAsc(asc);
                columnItemList.add(columnItem);
            }
        }
        sqlBean.setOrderbyBean(orderbyBean);
        return this;
    }

    /**
     * 获取ORDER BY信息
     */
    public ParseSelectService parseSelectLimit() throws JSQLParserException {
        if (sql == null) {
            return this;
        }
        Statement statement = CCJSqlParserUtil.parse(sql);
        Select selectStatement = (Select) statement;
        PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
        Limit limit = plain.getLimit();
        if (limit != null) {
            Expression offset = limit.getOffset();
            Expression rowCount = limit.getRowCount();
            long pageIndex = (long) getRightExpressionValue(offset);
            long pageSize = (long) getRightExpressionValue(rowCount);
            LimitBean limitBean = new LimitBean(pageIndex, pageSize);
            sqlBean.setLimitBean(limitBean);
        }
        return this;
    }


    private ColumnItem getLeftExpressionValue(Expression expression) {
        String tableAlias = null;
        String columnName = null;
        if (expression instanceof Column) {
            Column column = (Column) expression;
            columnName = column.getColumnName();
            Table table = column.getTable();
            if (table != null) {
                tableAlias = table.getName();
            }
            return new ColumnItem(tableAlias, columnName);
        }
        return null;
    }

    private List<JoinOn> parseExpression(Expression onExpression, String on) {
        List<JoinOn> joinOnList = new ArrayList<>();
        //操作符：=, >, >=, <, <=, <>
        String operator = null;
        //EqualsTo: =
        if (onExpression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) onExpression;
            operator = equalsTo.getStringExpression();
            //Column
            Expression leftExpression = equalsTo.getLeftExpression();
            //Column/StringValue,LongValue,DateValue等
            Expression rightExpression = equalsTo.getRightExpression();
            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            JoinOn joinOn = null;
            if (rightExpression instanceof Column) {
                ColumnItem rightValue = getLeftExpressionValue(rightExpression);
                joinOn = new JoinOn(on, leftValue, operator, rightValue);
            } else {
                Object value = getRightExpressionValue(rightExpression);
                joinOn = new JoinOn(on, leftValue, operator, value);
            }
            joinOnList.add(joinOn);
        }
        // AndExpression: and
        else if (onExpression instanceof AndExpression) {
            AndExpression and = (AndExpression) onExpression;
            operator = and.getStringExpression();
            Expression leftExpression = and.getLeftExpression();
            Expression rightExpression = and.getRightExpression();
            List<JoinOn> leftList = parseExpression(leftExpression, "ON");
            List<JoinOn> rightList = parseExpression(rightExpression, operator);
            joinOnList.addAll(leftList);
            joinOnList.addAll(rightList);
        }
        //OrExpression： or
        else if (onExpression instanceof OrExpression) {
            OrExpression or = (OrExpression) onExpression;
            operator = or.getStringExpression();
            Expression leftExpression = or.getLeftExpression();
            Expression rightExpression = or.getRightExpression();
            List<JoinOn> leftList = parseExpression(leftExpression, "ON");
            List<JoinOn> rightList = parseExpression(rightExpression, operator);
            joinOnList.addAll(leftList);
            joinOnList.addAll(rightList);
        }
        //Between： between and
        else if (onExpression instanceof Between) {
            Between between = (Between) onExpression;
            Expression leftExpression = between.getLeftExpression();
            Expression betweenExpressionStart = between.getBetweenExpressionStart();
            Expression betweenExpressionEnd = between.getBetweenExpressionEnd();

            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            Object startValue = getRightExpressionValue(betweenExpressionStart);
            Object endValue = getRightExpressionValue(betweenExpressionEnd);
            JoinOn joinOn = new JoinOn(on, leftValue, "BETWEEN", startValue + "@" + endValue);
            joinOnList.add(joinOn);

        }
        //LikeExpression: LIKE
        else if (onExpression instanceof LikeExpression) {
            LikeExpression like = (LikeExpression) onExpression;
            operator = like.getStringExpression();
            Expression leftExpression = like.getLeftExpression();
            Expression rightExpression = like.getRightExpression();

            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            Object value = getRightExpressionValue(rightExpression);
            JoinOn joinOn = new JoinOn(on, leftValue, operator, value);
            joinOnList.add(joinOn);

        }
        //InExpression: IN
        else if (onExpression instanceof InExpression) {
            InExpression in = (InExpression) onExpression;
            operator = "IN";
            Expression leftExpression = in.getLeftExpression();
            Expression rightExpression = in.getRightExpression();
            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            if (rightExpression != null) {

            }
            ItemsList rightItemsList = in.getRightItemsList();
            String value = rightItemsList.toString();
            JoinOn joinOn = new JoinOn(on, leftValue, operator, value);
            joinOnList.add(joinOn);

        }
        //GreaterThan: >
        else if (onExpression instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) onExpression;
            operator = greaterThan.getStringExpression();
            Expression leftExpression = greaterThan.getLeftExpression();
            Expression rightExpression = greaterThan.getRightExpression();

            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            Object value = getRightExpressionValue(rightExpression);
            JoinOn joinOn = new JoinOn(on, leftValue, operator, value);
            joinOnList.add(joinOn);

        }
        //GreaterThanEquals: >=
        else if (onExpression instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) onExpression;
            operator = greaterThanEquals.getStringExpression();
            Expression leftExpression = greaterThanEquals.getLeftExpression();
            Expression rightExpression = greaterThanEquals.getRightExpression();

            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            Object value = getRightExpressionValue(rightExpression);
            JoinOn joinOn = new JoinOn(on, leftValue, operator, value);
            joinOnList.add(joinOn);

        }
        //MinorThan: <
        else if (onExpression instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) onExpression;
            operator = minorThan.getStringExpression();
            Expression leftExpression = minorThan.getLeftExpression();
            Expression rightExpression = minorThan.getRightExpression();

            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            Object value = getRightExpressionValue(rightExpression);
            JoinOn joinOn = new JoinOn(on, leftValue, operator, value);
            joinOnList.add(joinOn);

        }
        //MinorThanEquals: <=
        else if (onExpression instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) onExpression;
            operator = minorThanEquals.getStringExpression();
            Expression leftExpression = minorThanEquals.getLeftExpression();
            Expression rightExpression = minorThanEquals.getRightExpression();

            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            Object value = getRightExpressionValue(rightExpression);
            JoinOn joinOn = new JoinOn(on, leftValue, operator, value);
            joinOnList.add(joinOn);

        }

        //IsNullExpression: IS NULL/IS NOT NULL
        else if (onExpression instanceof IsNullExpression) {
            IsNullExpression isNull = (IsNullExpression) onExpression;
            operator = isNull.isUseIsNull() ?
                    (isNull.isNot() ? " NOT" : "") + " ISNULL" :
                    " IS " + (isNull.isNot() ? "NOT " : "") + "NULL";
            Expression leftExpression = isNull.getLeftExpression();
            ColumnItem leftValue = getLeftExpressionValue(leftExpression);
            JoinOn joinOn = new JoinOn(on, leftValue, operator, "");
            joinOnList.add(joinOn);

        }
        //SimilarToExpression: SIMILAR TO
        else if (onExpression instanceof SimilarToExpression) {
            SimilarToExpression similarTo = (SimilarToExpression) onExpression;
            operator = similarTo.getStringExpression();
            Expression leftExpression = similarTo.getLeftExpression();
            Expression rightExpression = similarTo.getRightExpression();

        }
        return joinOnList;
    }

    private Object getRightExpressionValue(Expression expression) {
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        }
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        }
        if (expression instanceof DateValue) {
            Date date = ((DateValue) expression).getValue();
            return format.format(date);
        }
        if (expression instanceof TimeValue) {
            Time time = ((TimeValue) expression).getValue();
            return time.toString();
        }
        return null;
    }

    private static String getJoinType(Join join) {
        if (join.isInner()) {
            return "INNER JOIN";
        }
        if (join.isFull()) {
            return "FULL JOIN";
        }
        if (join.isLeft()) {
            return "LEFT JOIN";
        }
        if (join.isRight()) {
            return "RIGHT JOIN";
        }
        return "INNER JOIN";
    }


    public static void main(String[] args) throws JSQLParserException {
        ParseSelectService service = new ParseSelectService();
        String sql = "SELECT " +
                "t2.class_name className,\n" +
                "t.user_name as userName,\n" +
                "ui.phone,\n" +
                "ui.address,\n" +
                "(select u.phone,u.address from t_user_info u) ui,\n" +
                "count(t3.score) AS score\n" +
                "FROM t_user t \n" +
                "LEFT JOIN t_class t2 ON t2.id=t.class_id \n" +
                "and t2.id=12 and t2.name='aa' or t2.age>20\n" +
                "WHERE t2.user_id = t.id and t.user_name = 'zhangsan' \n" +
                "GROUP BY t2.class_name,t.user_name \n" +
                "ORDER BY t.id";

        String sql2 = "SELECT t.id\n " +
                "FROM t_user t \n" +
                "LEFT JOIN t_user_detail t2 ON t2.user_id=t.id \n" +
                "and t2.phone like '%zhangsan%'\n" +
                "WHERE t2.id = 6 or t.user_name = 'zhangsan' and t.id between 10 and 20\n" +
                "group by t.nick,t2.address \n" +
                "order by t.nick, t2.address desc \n" +
                "limit 0,10";

        SqlBean sqlBean = service.parse(sql);
        System.out.println(gson.toJson(sqlBean));

        System.out.println();
    }
}
