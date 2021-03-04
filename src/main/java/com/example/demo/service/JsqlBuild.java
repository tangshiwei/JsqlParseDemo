package com.demo.it.jsqlparse.build;

import com.demo.it.jsqlparse.bean.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class BuildSelectService {
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private PlainSelect plainSelect;
    private SqlBean sqlBean;

    public String build(SqlBean sqlBean) {
        this.sqlBean = sqlBean;
        plainSelect = new PlainSelect();


        buildSelect();
        buildFrom();
        buildJoin();
//        buildWhere();
//        buildGroupby();
//        buildOrderby();
//        buildLimit();
        String sql = plainSelect.toString();
        System.out.println(sql);
        return sql;
    }

    // 构建主表
    public BuildSelectService buildFrom() {
        List<TableBean> tableBeanList = sqlBean.getTableBeanList();
        if (tableBeanList == null || tableBeanList.size() == 0) {
            return this;
        }
        Table mainTable = getTable(tableBeanList.get(0));
        plainSelect.setFromItem(mainTable);
        return this;
    }

    // 构建select
    public BuildSelectService buildSelect() {
        List<ColumnBean> columnList = sqlBean.getColumnBeanList();
        if (columnList == null || columnList.size() == 0) {
            return this;
        }
        //创建查询的列
        List<SelectItem> selectItemList = new ArrayList<>();
        for (ColumnBean column : columnList) {
            //普通列
            SelectExpressionItem item = getColumn(column);
            //如果有函数
            if (StringUtils.isNotBlank(column.getFunctionName())) {
                Function columnFunction = getColumnFunction(column);
                item.setExpression(columnFunction);
            }
            selectItemList.add(item);
        }
        plainSelect.setSelectItems(selectItemList);
        return this;
    }

    // 构建Join
    public BuildSelectService buildJoin() {
        List<JoinBean> joinBeanList = sqlBean.getJoinBeanList();
        if (joinBeanList == null || joinBeanList.size() == 0) {
            return this;
        }
        List<Join> joinList = new ArrayList<>();

        for (int i = 0; i < joinBeanList.size(); i++) {
            JoinBean joinBean = joinBeanList.get(i);
            TableBean tableBean = joinBean.getTableBean();
            List<JoinOn> joinOnList = joinBean.getJoinOnList();

            Join join = new Join();
            join.setLeft(true);
            //
            Table joinTable = new Table();
            joinTable.setName(tableBean.getTableName());
            joinTable.setAlias(new Alias(tableBean.getTableAlias()));
            join.setRightItem(joinTable);
            //
            JoinOn joinOnFirst = joinOnList.get(0);
            Table tableLeft = getTable(joinOnFirst.getLeftTableAlias());
            Table tableRight = getTable(joinOnFirst.getRightTableAlias());

            EqualsTo equalsTo = new EqualsTo();
            equalsTo.setLeftExpression(new Column(tableLeft, joinOnFirst.getLeftColumnName()));
            equalsTo.setRightExpression(new Column(tableRight, joinOnFirst.getRightColumnName()));
            join.setOnExpression(equalsTo);

            joinList.add(join);
        }

        plainSelect.setJoins(joinList);
        return this;
    }

    // 构建Where
    public BuildSelectService buildWhere() {
        WhereBean whereBean = sqlBean.getWhereBean();
        if (whereBean == null) {
            return this;
        }
        List<JoinOn> joinOnList = whereBean.getJoinOnList();
        for (int i = 0; i < joinOnList.size() - 1; i++) {
            JoinOn joinOn = joinOnList.get(i);
            JoinOn joinOnNext = joinOnList.get(i + 1);

            EqualsTo leftEqualsTo = new EqualsTo();
            leftEqualsTo.setLeftExpression(getLeftColumn(joinOn));
            leftEqualsTo.setRightExpression(getRightValue(joinOn));
            plainSelect.setWhere(leftEqualsTo);

            EqualsTo rightEqualsTo = new EqualsTo();
            rightEqualsTo.setLeftExpression(getLeftColumn(joinOnNext));
            rightEqualsTo.setRightExpression(getRightValue(joinOnNext));

            Expression expression = getExpression(joinOnNext, leftEqualsTo, rightEqualsTo);
            plainSelect.setWhere(expression);
            System.out.println();
        }

        return this;
    }

    //生成左边的
    ////类型：Column
    // t.id=t2.user_id
    // t.id=10
    private Column getLeftColumn(JoinOn joinOn) {
        //类型：Column(t.id)
        Table table = new Table();
        table.setName(joinOn.getLeftTableAlias());
        Column column = new Column();
        column.setTable(table);
        column.setColumnName(joinOn.getLeftColumnName());
        return column;
    }

    private Column getRightColumn(JoinOn joinOn) {
        Table table = new Table();
        table.setName(joinOn.getRightTableAlias());
        Column column = new Column();
        column.setTable(table);
        column.setColumnName(joinOn.getRightColumnName());
        return column;
    }

    //生成右边
    //类型：StringValue,LongValue,DateValue等
    private Expression getRightValue(JoinOn joinOn) {
        Object value = joinOn.getValue();
        //1.常量值：t.id=10
        if (value != null) {
            if (value instanceof String) {
                return new StringValue(value.toString());
            }
            if (value instanceof Long) {
                return new LongValue(value.toString());
            }
            if (value instanceof Double) {
                return new DoubleValue(value.toString());
            }
            if (value instanceof Date) {
                return new DateValue(value.toString());
            }
            if (value instanceof Time) {
                return new TimeValue(value.toString());
            }
        }
        Column column = getRightColumn(joinOn);
        return column;
    }

    private Expression getExpression(JoinOn joinOn, Expression left, Expression right) {
        String on = joinOn.getOn();
        // AND / OR
        if ("AND".equalsIgnoreCase(on)) {
            AndExpression expression = new AndExpression();
            expression.setLeftExpression(left);
            expression.setRightExpression(right);
            return expression;
        } else if ("OR".equalsIgnoreCase(on)) {
            OrExpression expression = new OrExpression();
            expression.setLeftExpression(left);
            expression.setRightExpression(right);
            return expression;
        }
        return null;
    }

    //生成右边
    //类型：Column
    private Expression buildExpression(JoinOn joinOn, JoinOn joinOnNext) {
        String on = joinOn.getOn();
        String operator = joinOn.getOperator();
        Object value = joinOn.getValue();
        Expression leftExpression = null;
        Expression rightExpression = null;
        if ("=".equals(operator)) {
            EqualsTo temp = new EqualsTo();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));
            leftExpression = temp;
        } else if (">".equals(operator)) {
            GreaterThan temp = new GreaterThan();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));
            leftExpression = temp;
        } else if (">=".equals(operator)) {
            GreaterThanEquals temp = new GreaterThanEquals();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));
            leftExpression = temp;
        } else if ("<".equals(operator)) {
            MinorThan temp = new MinorThan();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));
            leftExpression = temp;
        } else if ("<=".equals(operator)) {
            MinorThanEquals temp = new MinorThanEquals();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));
            leftExpression = temp;
        } else if ("LIKE".equalsIgnoreCase(operator)) {
            LikeExpression temp = new LikeExpression();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));
            leftExpression = temp;
        } else if ("IN".equalsIgnoreCase(operator)) {
            InExpression temp = new InExpression();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setRightExpression(getRightValue(joinOn));

            leftExpression = temp;
        } else if ("BETWEEN".equalsIgnoreCase(operator)) {
            Between temp = new Between();
            temp.setLeftExpression(getLeftColumn(joinOn));
            if (value != null && StringUtils.isNotBlank(value.toString())) {
                String[] arr = value.toString().split("@");
                temp.setBetweenExpressionStart(new LongValue(arr[0]));
                temp.setBetweenExpressionEnd(new LongValue(arr[1]));
            }
            leftExpression = temp;
        } else if ("IS NULL".equalsIgnoreCase(operator) || "IS NOT NULL".equalsIgnoreCase(operator)) {
            IsNullExpression temp = new IsNullExpression();
            temp.setLeftExpression(getLeftColumn(joinOn));
            temp.setUseIsNull(operator.equals("IS NULL") ? false : true);
            temp.setNot(operator.equals("IS NULL") ? false : true);
            leftExpression = temp;
        }
        return leftExpression;
    }

    // 构建groupby
    public BuildSelectService buildGroupby() {
        GroupbyBean groupbyBean = sqlBean.getGroupbyBean();
        if (groupbyBean == null) {
            return this;
        }
        List<ColumnItem> columnItemList = groupbyBean.getColumnItemList();
        List<Expression> groupByExpressions = new ArrayList<>();
        columnItemList.forEach(column -> {
            Column c = new Column(getTable(column.getTableAlias()), column.getColumnName());
            groupByExpressions.add(c);
        });
        GroupByElement groupByElement = new GroupByElement();
        groupByElement.setGroupByExpressions(groupByExpressions);
        plainSelect.setGroupByElement(groupByElement);
        return this;
    }

    // 构建orderby
    public BuildSelectService buildOrderby() {
        OrderbyBean orderbyBean = sqlBean.getOrderbyBean();
        if (orderbyBean == null) {
            return this;
        }
        List<ColumnItem> columnItemList = orderbyBean.getColumnItemList();
        List<OrderByElement> orderByElements = new ArrayList<>();
        columnItemList.forEach(column -> {
            OrderByElement orderByElement = new OrderByElement();
            orderByElement.setAsc(column.isAsc());
            orderByElement.setExpression(new Column(getTable(column.getTableAlias()), column.getColumnName()));
            orderByElements.add(orderByElement);
        });
        plainSelect.setOrderByElements(orderByElements);
        return this;
    }

    // 构建分页Limit
    public BuildSelectService buildLimit() {
        LimitBean limitBean = sqlBean.getLimitBean();
        if (limitBean == null) {
            return this;
        }
        long pageIndex = limitBean.getPageIndex();
        long pageSize = limitBean.getPageSize();
        Limit limit = new Limit();
        long index = (pageIndex - 1) * pageSize;
        limit.setOffset(new LongValue(index));
        limit.setRowCount(new LongValue(pageSize));

        plainSelect.setLimit(limit);
        return this;
    }

    public static void main(String[] args) {
        BuildSelectService service = new BuildSelectService();
        String sqlBeanJson = "{\"tableBeanList\":[{\"tableName\":\"t_user\",\"tableAlias\":\"t\"},{\"tableName\":\"t_class\",\"tableAlias\":\"t2\"}],\"columnBeanList\":[{\"columnName\":\"class_name\",\"columnAlis\":\"className\",\"tableAlias\":\"t2\",\"functionName\":\"\",\"asc\":true},{\"columnName\":\"user_name\",\"columnAlis\":\"userName\",\"tableAlias\":\"t\",\"functionName\":\"\",\"asc\":true},{\"columnName\":\"phone\",\"columnAlis\":\"\",\"tableAlias\":\"ui\",\"functionName\":\"\",\"asc\":true},{\"columnName\":\"address\",\"columnAlis\":\"\",\"tableAlias\":\"ui\",\"functionName\":\"\",\"asc\":true},{\"columnName\":\"(SELECT u.phone, u.address FROM t_user_info u)\",\"columnAlis\":\"ui\",\"tableAlias\":\"\",\"functionName\":\"\",\"asc\":true},{\"columnName\":\"score\",\"columnAlis\":\"score\",\"tableAlias\":\"t3\",\"functionName\":\"count\",\"asc\":true}],\"joinBeanList\":[{\"joinType\":\"LEFT JOIN\",\"tableBean\":{\"tableName\":\"t_class\",\"tableAlias\":\"t2\"},\"joinOnList\":[{\"leftTableAlias\":\"t2\",\"leftColumnName\":\"id\",\"operator\":\"=\",\"on\":\"ON\",\"rightTableAlias\":\"t\",\"rightColumnName\":\"class_id\"},{\"leftTableAlias\":\"t2\",\"leftColumnName\":\"id\",\"operator\":\"=\",\"on\":\"AND\",\"value\":12},{\"leftTableAlias\":\"t2\",\"leftColumnName\":\"name\",\"operator\":\"=\",\"on\":\"AND\",\"value\":\"aa\"},{\"leftTableAlias\":\"t2\",\"leftColumnName\":\"age\",\"operator\":\">\",\"on\":\"OR\",\"value\":20}]}],\"groupbyBean\":{\"columnItemList\":[{\"tableAlias\":\"t2\",\"columnName\":\"class_name\",\"asc\":false},{\"tableAlias\":\"t\",\"columnName\":\"user_name\",\"asc\":false}]},\"whereBean\":{\"joinOnList\":[{\"leftTableAlias\":\"t2\",\"leftColumnName\":\"user_id\",\"operator\":\"=\",\"on\":\"ON\",\"rightTableAlias\":\"t\",\"rightColumnName\":\"id\"},{\"leftTableAlias\":\"t\",\"leftColumnName\":\"user_name\",\"operator\":\"=\",\"on\":\"AND\",\"value\":\"zhangsan\"}]},\"orderbyBean\":{\"columnItemList\":[{\"tableAlias\":\"t\",\"columnName\":\"id\",\"asc\":true}]}}\n";
        SqlBean sqlBean = gson.fromJson(sqlBeanJson, SqlBean.class);
        System.out.println();
        service.build(sqlBean);
        System.out.println();
    }


    private Table getTable(TableBean tableBean) {
        return getTable(tableBean.getTableName(), tableBean.getTableAlias());
    }

    private Table getTable(String tableAlias) {
        return getTable(null, tableAlias);
    }

    private Table getTable(String tableName, String tableAlias) {
        Table table = new Table(tableName);
        table.setAlias(new Alias(tableAlias));
        return table;
    }

    private SelectExpressionItem getColumn(ColumnBean columnBean) {
        SelectExpressionItem item = new SelectExpressionItem();
        Table table = getTable(columnBean.getTableAlias());
        item.setExpression(new Column(table, columnBean.getColumnName()));
        item.setAlias(new Alias(columnBean.getColumnAlis()));
        return item;
    }

    private Function getColumnFunction(ColumnBean columnBean) {
        Function function = new Function();
        function.setName(columnBean.getFunctionName());
        ExpressionList list = new ExpressionList();
        Column column = new Column(getTable(columnBean.getTableAlias()), columnBean.getColumnName());
        list.setExpressions(Arrays.asList(column));
        function.setParameters(list);
        return function;
    }
}
