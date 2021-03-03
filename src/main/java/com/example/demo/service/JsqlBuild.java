package com.huawei.it.jsqlparse.build;

import com.huawei.it.jsqlparse.bean.*;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class BuildSelectService {
    private PlainSelect plainSelect;

    private void init() {
        if (plainSelect == null)
            plainSelect = new PlainSelect();
    }

    // 构建主表
    public BuildSelectService buildFrom(TableBean main) {
        init();
        plainSelect = new PlainSelect();
        Table mainTable = getTable(main);
        plainSelect.setFromItem(mainTable);
        return this;
    }

    // 构建select
    public BuildSelectService buildSelect(List<ColumnBean> columnList) {
        init();
        //创建查询的列
        List<SelectItem> selectItemList = new ArrayList<>();
        for (ColumnBean column : columnList) {
            //普通列
            SelectExpressionItem item = getColumn(column);
            selectItemList.add(item);
            //如果有函数
            if (StringUtils.isNotBlank(column.getFunctionName())) {
                Function columnFunction = getColumnFunction(column);
                item.setExpression(columnFunction);
            }
        }
        plainSelect.setSelectItems(selectItemList);
        return this;
    }

    // 构建Join
    public BuildSelectService buildTableJoin(List<TableJoinBean> tableJoinBeanList) {
        List<Join> joinList = new ArrayList<>();
        tableJoinBeanList.forEach(tableJoin -> {
            //join
            Table leftTable = getTable(tableJoin.getLeftTable());
            Table rightTable = getTable(tableJoin.getRightTable());
            Join join = new Join();
            join.setLeft(tableJoin.isLeft());
            join.setRightItem(rightTable);
            //on
            EqualsTo equalsTo = new EqualsTo();
            equalsTo.setLeftExpression(new Column(leftTable, tableJoin.getLeftColumnName()));
            equalsTo.setRightExpression(new Column(rightTable, tableJoin.getRightColumnName()));
            join.setOnExpression(equalsTo);
            // and where

            joinList.add(join);
        });

        plainSelect.setJoins(joinList);
        return this;
    }

    //    //条件
//    EqualsTo leftEqualsTo = new EqualsTo();
//        leftEqualsTo.setLeftExpression(new Column(table, "f1"));
//    StringValue stringValue = new StringValue("1222121");
//        leftEqualsTo.setRightExpression(stringValue);
//        plainSelect.setWhere(leftEqualsTo);
//
//    EqualsTo rightEqualsTo = new EqualsTo();
//        rightEqualsTo.setLeftExpression(new Column(table, "f2"));
//    StringValue stringValue1 = new StringValue("122212111111");
//        rightEqualsTo.setRightExpression(stringValue1);
//    OrExpression orExpression = new OrExpression(leftEqualsTo, rightEqualsTo);
//        plainSelect.setWhere(orExpression);
    // 构建Where
    public BuildSelectService buildWhere(WhereBean whereBean) {
        init();
        List<JoinOn> joinOnList = whereBean.getJoinOnList();
        for (JoinOn joinOn : joinOnList) {
            //连接符：(on, and ,or)
            String on = joinOn.getOn();
            String tableAlias = joinOn.getLeftTableAlias();
            String leftColumnName = joinOn.getLeftColumnName();
            //操作符：=, >, >=, <, <=, <>
            String operator = joinOn.getOperator();

            Expression expression = null;
            plainSelect.setWhere(expression);
        }

        return this;
    }

    private Expression getColumnLeftExpression(JoinOn joinOn) {
        Table table = new Table();
        table.setName(joinOn.getLeftTableAlias());
        Column column = new Column();
        column.setTable(table);
        column.setColumnName(joinOn.getLeftColumnName());
        return column;
    }
    private Expression getColumnRightExpression(JoinOn joinOn) {
        Table table = new Table();
        table.setName(joinOn.getRightTableAlias());
        Column column = new Column();
        column.setTable(table);
        column.setColumnName(joinOn.getRightColumnName());
        return column;
    }
    private Expression getRightExpression(JoinOn joinOn) {
        String rightTableAlias = joinOn.getRightTableAlias();
        String rightColumnName = joinOn.getRightColumnName();
        Object value = joinOn.getValue();
        //1.t.id=10
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
            return null;
        }
        //2. t.id=t2.user_id

        Expression expression = getColumnRightExpression(joinOn);
        return expression;
    }

    private Expression getExpression(JoinOn joinOn) {
        String operator = joinOn.getOperator();
        if ("=".equals(operator)) {
            EqualsTo leftEqualsTo = new EqualsTo();
            leftEqualsTo.setLeftExpression(getColumnLeftExpression(joinOn));
            leftEqualsTo.setRightExpression(getColumnRightExpression(joinOn));


        } else if (">".equals(operator)) {
            GreaterThan expression = new GreaterThan();

        } else if (">=".equals(operator)) {
            GreaterThanEquals expression = new GreaterThanEquals();

        } else if ("<".equals(operator)) {
            MinorThan expression = new MinorThan();

        } else if ("<=".equals(operator)) {
            MinorThanEquals expression = new MinorThanEquals();

        } else if ("AND".equalsIgnoreCase(operator)) {
            AndExpression expression = new AndExpression();

        } else if ("OR".equalsIgnoreCase(operator)) {
            OrExpression expression = new OrExpression();

        } else if ("LIKE".equalsIgnoreCase(operator)) {
            LikeExpression expression = new LikeExpression();

        } else if ("IN".equalsIgnoreCase(operator)) {
            InExpression expression = new InExpression();


        } else if ("BETWEEN".equalsIgnoreCase(operator)) {
            Between expression = new Between();


        } else if ("IS NULL".equalsIgnoreCase(operator) || "IS NOT NULL".equalsIgnoreCase(operator)) {
            IsNullExpression expression = new IsNullExpression();

        }
        return null;
    }




    public static void main(String[] args) {

    }

    // 构建groupby
    public BuildSelectService buildGroupby(List<ColumnBean> columnList) {
        init();
        List<Expression> groupByExpressions = new ArrayList<>();
        columnList.forEach(column -> {
            Column c = new Column(getTable(column.getTableBean()), column.getColumnName());
            groupByExpressions.add(c);
        });
        GroupByElement groupByElement = new GroupByElement();
        groupByElement.setGroupByExpressions(groupByExpressions);
        plainSelect.setGroupByElement(groupByElement);
        return this;
    }

    // 构建orderby
    public BuildSelectService buildOrderby(List<ColumnBean> columnList) {
        init();
        List<OrderByElement> orderByElements = new ArrayList<>();
        columnList.forEach(column -> {
            OrderByElement orderByElement = new OrderByElement();
            orderByElement.setAsc(column.isAsc());
            orderByElement.setExpression(new Column(getTable(column.getTableBean()), column.getColumnName()));
            orderByElements.add(orderByElement);
        });
        plainSelect.setOrderByElements(orderByElements);
        return this;
    }

    // 构建分页Limit
    public BuildSelectService buildLimit(int pageIndex, int pageSize) {
        init();
        Limit limit = new Limit();
        int index = (pageIndex - 1) * pageSize;
        limit.setOffset(new LongValue(index));
        limit.setRowCount(new LongValue(pageSize));

        plainSelect.setLimit(limit);
        return this;
    }

    public String buildSql() {
        String sql = plainSelect.toString();
        System.out.println(sql);
        return sql;
    }

    public static void main222(String[] args) {
        BuildSelectService service = new BuildSelectService();

        TableBean main = new TableBean("table", "t");
        List<ColumnBean> columnBeanList = new ArrayList<>();
        ColumnBean column = new ColumnBean();
        column.setColumnAlis("name");
        column.setColumnName("f_name");
        column.setTableBean(main);
        columnBeanList.add(column);
        column = new ColumnBean();
        column.setColumnAlis("age");
        column.setColumnName("f_age");
        column.setTableBean(main);
        column.setFunctionName("COUNT");
        columnBeanList.add(column);
        //join
        List<TableJoinBean> tableJoinBeanList = new ArrayList<>();
        TableBean tableBean3 = new TableBean("table", "t");
        TableBean tableBean4 = new TableBean("table2", "t2");
        TableJoinBean joinBean = new TableJoinBean(tableBean3, tableBean4, "id", "id2");
        tableJoinBeanList.add(joinBean);
        //
        tableBean3 = new TableBean("table2", "t2");
        tableBean4 = new TableBean("table3", "t3");
        joinBean = new TableJoinBean(tableBean3, tableBean4, "id2", "id3");
        tableJoinBeanList.add(joinBean);
//        //where
//        List<WhereBean> whereBeanList = new ArrayList<>();
//        TableBean table = new TableBean("table", "t");
//        WhereBean whereBean = new WhereBean(table, "id", "=", 123);
//        whereBeanList.add(whereBean);
//        //
//        table = new TableBean("table", "t");
//        whereBean = new WhereBean(table, "id", "<", 10);
//        //whereBean.setOr(true);
//        whereBeanList.add(whereBean);
        //groupby
        List<ColumnBean> groupbyList = new ArrayList<>();
        groupbyList.add(new ColumnBean(tableBean3, "name"));
        groupbyList.add(new ColumnBean(tableBean3, "name3"));
        //orderby
        List<ColumnBean> orderbyList = new ArrayList<>();
        orderbyList.add(new ColumnBean(tableBean3, "age"));
        orderbyList.add(new ColumnBean(tableBean3, "address"));

        service.buildFrom(main)
                .buildSelect(columnBeanList)
                .buildTableJoin(tableJoinBeanList)
                //.buildWhere(whereBeanList)
                .buildGroupby(groupbyList)
                .buildOrderby(orderbyList)
                .buildLimit(1, 10)
                .buildSql();

        String sql = "SELECT t.f_name AS name, COUNT(t.f_age) AS age " +
                "FROM table AS t " +
                "LEFT JOIN table2 AS t2 ON t.id = t2.id2 " +
                "LEFT JOIN table3 AS t3 ON t2.id2 = t3.id3 " +
                "WHERE t.id = '123";

    }


    private Table getTable(TableBean tableBean) {
        Table table = new Table(tableBean.getTableName());
        table.setAlias(new Alias(tableBean.getTableAlias()));
        return table;
    }

    private SelectExpressionItem getColumn(ColumnBean columnBean) {
        SelectExpressionItem item = new SelectExpressionItem();
        Table table = getTable(columnBean.getTableBean());
        item.setExpression(new Column(table, columnBean.getColumnName()));
        item.setAlias(new Alias(columnBean.getColumnAlis()));
        return item;
    }

    private Function getColumnFunction(ColumnBean columnBean) {
        Function function = new Function();
        function.setName(columnBean.getFunctionName());
        ExpressionList list = new ExpressionList();
        Column column = new Column(getTable(columnBean.getTableBean()), columnBean.getColumnName());
        list.setExpressions(Arrays.asList(column));
        function.setParameters(list);
        return function;
    }
}
