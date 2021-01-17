package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.commons.lang3.StringUtils;

public class WhereItem implements BaseParse {
    //左边表关联
    private String leftTableAlias;
    private String leftFieldName;
    // 操作符：=,<,>
    private String op = "=";
    //右边表关联
    private String rightableAlias;
    private String rightFieldName;
    //右边值
    private String value;
    //子查询
    private SqlBean sqlBean;

    @Override
    public void parseSql(String subSql) {
        if (subSql.indexOf("=") != -1) {
            op = " = ";
        } else if (subSql.indexOf("<") != -1) {
            op = " < ";
        } else if (subSql.indexOf(">") != -1) {
            op = " > ";
        } else if (subSql.indexOf("<=") != -1) {
            op = " < ";
        } else if (subSql.indexOf(">=") != -1) {
            op = " >= ";
        } else if (subSql.toLowerCase().indexOf(" in") != -1) {
            op = " IN ";
        }
        String[] split = subSql.split(op);
        String left = split[0];
        String right = split[1];
        String[] split1 = left.split("\\.");
        leftTableAlias = split1[0];
        leftFieldName = split1[1];
        if (op.trim().equals("IN")) {
            value = right;
        }
        //1.子查询
        else if (right.toLowerCase().startsWith("(select")) {

        }
        //2.关联表达式
        else if (right.indexOf(".") != -1) {
            String[] split2 = right.split("\\.");
            rightableAlias = split2[0];
            rightFieldName = split2[1];
        }
        //3.常量值
        else {
            value = right;
        }
        System.out.println();
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer();
        //左边
        if (StringUtils.isNotBlank(leftTableAlias)) {
            sb.append(leftTableAlias).append(".").append(leftFieldName).append(op);
        } else {
            sb.append(leftTableAlias).append(op);
        }
        //右边
        if (sqlBean != null) {

        } else if (StringUtils.isNotBlank(rightableAlias) && StringUtils.isNotBlank(rightFieldName)) {
            sb.append(leftTableAlias).append(".").append(leftFieldName);
        } else {
            sb.append(value);
        }
        return sb.toString();
    }
}
