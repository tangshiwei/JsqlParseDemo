package com.huawei.it.parse;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.SelectUtils;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsqlParseDemo {

    public static void main(String[] args) {
        createSelect();
//        changeSelect();
//        createInsert();
//        createUpdate();
//        createDelete();
        String sql1="SELECT t.f1, t.f2, count(t.f1) AS count FROM table AS t " +
                "LEFT JOIN table2 AS t2 ON t.f1 = t2.f2 " +
                "LEFT JOIN table3 AS t3 ON t.f1 = t3.f2 " +
                "WHERE t.f1 = '1222121' OR t.f2 = '122212111111' " +
                "GROUP BY t.f1\n";
    }

    public static void createSelect() {
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
        System.out.println("==================================================创建查询====================================================");
    }


    //在原有的sql基础上改
    public static void changeSelect() {
        System.out.println("==================================================改变原有查询====================================================");
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        try {
            Select select = (Select) (parserManager.parse(new StringReader("select * from table")));
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
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
            LongValue stringValue = new LongValue("12");
            leftEqualsTo.setRightExpression(stringValue);

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
//            System.out.println(SQLFormatterUtil.format(plainSelect.toString()));
            System.out.println(plainSelect.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        System.out.println("==================================================改变原有查询====================================================");
    }

    //创建插入sql语句
    public static void createInsert() {
        System.out.println("==================================================创建插入语句====================================================");
        Insert insert = new Insert();
        Table table = new Table();
        table.setName("table");
        insert.setTable(table);
        insert.setColumns(Arrays.asList(
                new Column(table, "f1"),
                new Column(table, "f2"),
                new Column(table, "f3")
        ));

        MultiExpressionList multiExpressionList = new MultiExpressionList();
        multiExpressionList.addExpressionList(Arrays.asList(
                new StringValue("1"),
                new StringValue("2"),
                new StringValue("3")
        ));
        insert.setItemsList(multiExpressionList);
        System.out.println(insert);
        System.out.println("==================================================创建插入语句====================================================");
    }

    //创建插入sql语句
    public static void createUpdate() {
        System.out.println("==================================================创建更新语句====================================================");
        Update update = new Update();
        Table table = new Table();
        table.setName("table");
        update.setTable(table);
        update.setColumns(Arrays.asList(
                new Column(table, "f1"),
                new Column(table, "f2"),
                new Column(table, "f3")
        ));
        update.setExpressions(Arrays.asList(
                new StringValue("1"),
                new StringValue("6"),
                new StringValue("2")
        ));
        //条件
        EqualsTo leftEqualsTo = new EqualsTo();
        leftEqualsTo.setLeftExpression(new Column(table, "f1"));
        StringValue stringValue = new StringValue("1222121");
        leftEqualsTo.setRightExpression(stringValue);
        EqualsTo rightEqualsTo = new EqualsTo();
        rightEqualsTo.setLeftExpression(new Column(table, "f2"));
        StringValue stringValue1 = new StringValue("122212111111");
        rightEqualsTo.setRightExpression(stringValue1);
        OrExpression orExpression = new OrExpression(leftEqualsTo, rightEqualsTo);
        update.setWhere(orExpression);
        System.out.println(update);
        System.out.println("==================================================创建更新语句====================================================");
    }

    //创建插入sql语句
    public static void createDelete() {
        System.out.println("==================================================创建删除语句====================================================");
        Delete delete = new Delete();
        Table table = new Table();
        table.setName("table");
        delete.setTable(table);
        //条件
        EqualsTo leftEqualsTo = new EqualsTo();
        leftEqualsTo.setLeftExpression(new Column(table, "f1"));
        StringValue stringValue = new StringValue("1222121");
        leftEqualsTo.setRightExpression(stringValue);
        EqualsTo rightEqualsTo = new EqualsTo();
        rightEqualsTo.setLeftExpression(new Column(table, "f2"));
        StringValue stringValue1 = new StringValue("122212111111");
        rightEqualsTo.setRightExpression(stringValue1);
        OrExpression orExpression = new OrExpression(leftEqualsTo, rightEqualsTo);
        delete.setWhere(orExpression);
        System.out.println(delete);
        System.out.println("==================================================创建删除语句====================================================");
    }
}
