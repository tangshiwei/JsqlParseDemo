package com.example.demo.service;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.StringReader;
import java.util.List;


public class BaseUtil {

   public  static String sql1 = "select " +
            "t1.id as id," +
            "t1.username username," +
            "t1.password," +
            "t2.phone," +
            "t3.address," +
            "(select t4.price from t_price t4) price" +
            " from t_user t1" +

            " left join t_user_info t2 on t1.id=t2.user_id and t1.age=25 and t2.qq='234533'" +
            " left join t_user_address t3 on t1.id=t3.user_id" +

            " where t1.time='2021-01-17 17:45:56' " +
            " and t1.id=t2.user_id " +
            " and t3.name in('aa','bb') " +
            " group by t1.id,t3.name " +
            " order by t1.id,t2.phone " +
            " limit 10";

    public static String sql2 = "select " +
            "t1.id as id," +
            "t1.username username," +
            "t1.password," +
            "t2.phone," +
            "t3.address," +
            "(select t4.price from t_price t4) price" +
            " from t_user t1,t_user_info t2, t_user_address t3" +
            " where t1.id=12" +
            " group by t1.id,t3.name " +
            " order by t1.id,t2.phone " +
            " limit 0,10";

    public  static Select getStatement(String sql) {
        Select select = null;
        try {
            Statement sqlStmt = CCJSqlParserUtil.parse(new StringReader(sql));
            select = (Select) sqlStmt;
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        return select;
    }

    public static FromItem getFromItem(String sql) {
        Select statement = getStatement(sql);
        SelectBody selectBody = statement.getSelectBody();
        return getFromItem(selectBody);
    }

    public static FromItem getFromItem(SelectBody selectBody) {
        if (selectBody instanceof PlainSelect) {
            PlainSelect select = (PlainSelect) selectBody;
            FromItem fromItem = select.getFromItem();
            return fromItem;
        } else if (selectBody instanceof WithItem) {
            WithItem select = (WithItem) selectBody;
            getFromItem(select.getSelectBody());
        }
        return null;
    }


    public static List<SelectItem> getSelectItems(String sql) {
        List<SelectItem> list = null;
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(sql));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            list = plain.getSelectItems();
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        return list;
    }


    public static List<Join> getSelectJoin(String sql) {
        List<Join> list = null;
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            Select selectStatement = (Select) statement;
            PlainSelect plain = (PlainSelect) selectStatement.getSelectBody();
            list = plain.getJoins();

            if (list != null) {
                for (int i = 0; i < list.size(); i++) {
                    //注意leftjoin rightjoin 等等的to string()区别
                    System.out.println(list.get(i).toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static String getSelectWhere(String sql) {
        String str = null;
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(sql));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            Expression where_expression = plain.getWhere();
            str = where_expression.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return str;
    }

    public static List<Expression> getSelectGroupby(String sql) {
        List<Expression> list = null;
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(sql));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            list = plain.getGroupByColumnReferences();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static List<OrderByElement> getSelectOrderby(String sql) {
        List<OrderByElement> list = null;
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(sql));
            PlainSelect plain = (PlainSelect) select.getSelectBody();
            list = plain.getOrderByElements();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static Limit getLimit(String sql) {
        Limit limit = null;
        try {
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            Select select = (Select) parserManager.parse(new StringReader(sql));
            SelectBody selectBody = select.getSelectBody();
            if (selectBody instanceof PlainSelect) {
                limit = ((PlainSelect) selectBody).getLimit();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return limit;
    }
}
