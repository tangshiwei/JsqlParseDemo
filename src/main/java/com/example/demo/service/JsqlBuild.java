package com.huawei.it.parse;

import com.huawei.it.parse.bean.ColumnBean;
import com.huawei.it.parse.bean.TableBean;
import com.huawei.it.parse.bean.TableJoinBean;
import com.huawei.it.parse.bean.WhereBean;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class JsqlParseBuildService {
    public void createSelect() {
        System.out.println("==================================================创建查询====================================================");
        PlainSelect plainSelect = new PlainSelect();
        //创建查询的表
        Table table = new Table("table");
        table.setAlias(new Alias("t"));
        plainSelect.setFromItem(table);
        //创建查询的列
        List<String> selectColumnsStr = Arrays.asList("f1", "f2");

        List<SelectItem> expressionItemList = selectColumnsStr.stream().map(item -> {
            SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
            selectExpressionItem.setExpression(new Column(table, item));
            return (SelectItem) selectExpressionItem;
        }).collect(Collectors.toList());

        SelectExpressionItem selectExpressionItem = new SelectExpressionItem();
        selectExpressionItem.setAlias(new Alias("count"));
        Function function = new Function();
        function.setName("count");
        ExpressionList expressionList = new ExpressionList();
        expressionList.setExpressions(Arrays.asList(new Column(table, "f1")));
        function.setParameters(expressionList);
        selectExpressionItem.setExpression(function);
        expressionItemList.add(selectExpressionItem);

        plainSelect.setSelectItems(expressionItemList);

        AtomicInteger atomicInteger = new AtomicInteger(1);
        List<Join> joinList = Stream.of(new String[2]).map(item -> {
            Join join = new Join();
            join.setLeft(true);
            Table joinTable = new Table();
            joinTable.setName("table" + atomicInteger.incrementAndGet());
            joinTable.setAlias(new Alias("t" + atomicInteger.get()));
            join.setRightItem(joinTable);
            EqualsTo equalsTo = new EqualsTo();
            equalsTo.setLeftExpression(new Column(table, "f1"));
            equalsTo.setRightExpression(new Column(joinTable, "f2"));
            join.setOnExpression(equalsTo);
            return join;
        }).collect(Collectors.toList());
        plainSelect.setJoins(joinList);


        //条件
        EqualsTo leftEqualsTo = new EqualsTo();
        leftEqualsTo.setLeftExpression(new Column(table, "f1"));
        StringValue stringValue = new StringValue("1222121");
        leftEqualsTo.setRightExpression(stringValue);
        plainSelect.setWhere(leftEqualsTo);

        EqualsTo rightEqualsTo = new EqualsTo();
        rightEqualsTo.setLeftExpression(new Column(table, "f2"));
        StringValue stringValue1 = new StringValue("122212111111");
        rightEqualsTo.setRightExpression(stringValue1);
        OrExpression orExpression = new OrExpression(leftEqualsTo, rightEqualsTo);
        plainSelect.setWhere(orExpression);

        //分组
        GroupByElement groupByElement = new GroupByElement();
        groupByElement.setGroupByExpressions(Arrays.asList(new Column(table, "f1")));
        plainSelect.setGroupByElement(groupByElement);
        System.out.println(plainSelect);

        //排序
        OrderByElement orderByElement = new OrderByElement();
        orderByElement.setAsc(true);
        orderByElement.setExpression(new Column(table, "f1"));
        OrderByElement orderByElement1 = new OrderByElement();
        orderByElement1.setAsc(false);
        orderByElement1.setExpression(new Column(table, "f2"));

        //分页
        Limit limit = new Limit();
        limit.setRowCount(new LongValue(2));
        limit.setOffset(new LongValue(10));
        plainSelect.setLimit(limit);
        plainSelect.setOrderByElements(Arrays.asList(orderByElement, orderByElement1));
        System.out.println(plainSelect.toString());

        String sql = "SELECT t.f1, t.f2, count(t.f1) AS count " +
                "FROM table AS t " +
                "LEFT JOIN table2 AS t2 ON t.f1 = t2.f2 " +
                "LEFT JOIN table3 AS t3 ON t.f1 = t3.f2 " +
                "WHERE t.f1 = '1222121' OR t.f2 = '122212111111' " +
                "GROUP BY t.f1 " +
                "ORDER BY t.f1, t.f2 DESC " +
                "LIMIT 10, 2\n";
    }

    private PlainSelect plainSelect;

    private void init() {
        if (plainSelect == null)
            plainSelect = new PlainSelect();
    }

    // 构建主表
    public JsqlParseBuildService buildFrom(TableBean main) {
        init();
        plainSelect = new PlainSelect();
        Table mainTable = getTable(main);
        plainSelect.setFromItem(mainTable);
        return this;
    }

    // 构建select
    public JsqlParseBuildService buildSelect(List<ColumnBean> columnList) {
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
    public JsqlParseBuildService buildTableJoin(List<TableJoinBean> tableJoinBeanList) {
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

    // 构建Where
    public JsqlParseBuildService buildWhere(List<WhereBean> whereBeanList) {
        init();
        EqualsTo firstEqualsTo = null;
        for (int i = 0; i < whereBeanList.size(); i++) {
            WhereBean where = whereBeanList.get(i);
            EqualsTo rightEqualsTo = new EqualsTo();
            rightEqualsTo.setLeftExpression(new Column(getTable(where.getTable()), where.getColumnName()));
            Expression valueExpression = null;
            Object value = where.getValue();
            if (value instanceof String) {
                valueExpression = new StringValue(value.toString());
            } else {
                valueExpression = new LongValue(Long.valueOf(value.toString()));
            }
            rightEqualsTo.setRightExpression(valueExpression);
            if (i == 0) {
                firstEqualsTo = rightEqualsTo;
            }
            BinaryExpression binaryExpression = null;
            //OR
            if (where.isOr()) {
                binaryExpression = new OrExpression(firstEqualsTo, rightEqualsTo);
            }
            // AND
            else {
                binaryExpression = new AndExpression(firstEqualsTo, rightEqualsTo);
            }
            plainSelect.setWhere(binaryExpression);
        }

        return this;
    }

    // 构建groupby
    public JsqlParseBuildService buildGroupby(List<ColumnBean> columnList) {
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
    public JsqlParseBuildService buildOrderby(List<ColumnBean> columnList) {
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
    public JsqlParseBuildService buildLimit(int pageIndex, int pageSize) {
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

    public static void main(String[] args) {
        JsqlParseBuildService service = new JsqlParseBuildService();

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
        //where
        List<WhereBean> whereBeanList = new ArrayList<>();
        TableBean table = new TableBean("table", "t");
        WhereBean whereBean = new WhereBean(table, "id", "=", 123);
        whereBeanList.add(whereBean);
        //
        table = new TableBean("table", "t");
        whereBean = new WhereBean(table, "id", "<", 10);
        //whereBean.setOr(true);
        whereBeanList.add(whereBean);
        //groupby
        List<ColumnBean> groupbyList = new ArrayList<>();
        groupbyList.add(new ColumnBean(table, "name"));
        groupbyList.add(new ColumnBean(tableBean3, "name3"));
        //orderby
        List<ColumnBean> orderbyList = new ArrayList<>();
        orderbyList.add(new ColumnBean(table, "age"));
        orderbyList.add(new ColumnBean(tableBean3, "address"));

        service.buildFrom(main)
                .buildSelect(columnBeanList)
                .buildTableJoin(tableJoinBeanList)
                .buildWhere(whereBeanList)
                .buildGroupby(groupbyList)
                .buildOrderby(orderbyList)
                .buildLimit(1, 10)
                .buildSql();

        service.createSelect();

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
