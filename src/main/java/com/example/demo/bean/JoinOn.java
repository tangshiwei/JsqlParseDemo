package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import org.apache.commons.lang3.StringUtils;

public class JoinOn extends BaseBean implements BaseParse {
    //左边表关联
    private String leftTableAlias;
    private String leftFieldName;
    private String op = "=";
    //右边表关联
    private String rightableAlias;
    private String rightFieldName;
    //常量值
    private String value;

    @Override
    public void parseSql(String onExpression) {
        this.sql = onExpression;
        String[] split = null;
        if (onExpression.indexOf("=") != -1) {
            split = onExpression.split("=");
        }
        if (split == null || split.length == 0) {
            return;
        }
        String left = split[0].trim();
        String right = split[1].trim();
        String[] split1 = left.split("\\.");
        leftTableAlias = split1[0];
        leftFieldName = split1[1];
        //右边如果是表达式
        if (right.indexOf(".") != -1) {
            String[] split2 = right.split("\\.");
            rightableAlias = split2[0];
            rightFieldName = split2[1];
        }
        //常量值
        else {
            value = right;
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer();
        sb.append(leftTableAlias).append(".").append(leftFieldName);
        sb.append(op);
        if (StringUtils.isNotBlank(rightableAlias) && StringUtils.isNotBlank(rightFieldName)) {
            sb.append(rightableAlias).append(".").append(rightFieldName);
        } else {
            sb.append(value);
        }
        return sb.toString();
    }
}
