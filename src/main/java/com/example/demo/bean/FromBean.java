package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import com.example.demo.service.BaseUtil;

public class FromBean extends TableBean implements BaseParse {

    public static void main(String[] args) {
        FromBean fromBean = new FromBean();
        fromBean.parseSql(BaseUtil.sql1);
        String str = fromBean.buildSql();
        System.out.println(str);
    }

    @Override
    public void parseSql(String sql) {
        String subSql = BaseUtil.getFromItem(sql).toString();
        String[] split = subSql.split(" ");
        if (split == null || split.length == 0) {
            return;
        }
        if (split.length == 1) {
            setTableName(split[0]);
        } else if (split.length == 2) {
            setTableName(split[0]);
            setTableAlias(split[1]);
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer(line);
        sb.append("FROM ");
        sb.append(super.buildSql());

        return sb.toString();
    }
}
