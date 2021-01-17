package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import com.example.demo.service.BaseUtil;

import java.util.ArrayList;
import java.util.List;

public class WhereBean extends BaseBean implements BaseParse {
    private List<WhereItem> whereSubList = new ArrayList<>();

    public static void main(String[] args) {
        WhereBean bean = new WhereBean();
        bean.parseSql(BaseUtil.sql1);
        String s = bean.buildSql();
        System.out.println(s);
    }

    @Override
    public void parseSql(String sql) {
        String subSql = BaseUtil.getSelectWhere(sql);
        this.sql = subSql;
        String[] split = subSql.split(" AND|and ");
        for (String str : split) {
            WhereItem sub = new WhereItem();
            sub.parseSql(str);
            whereSubList.add(sub);
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer(line);
        sb.append("WHERE  ");
        for (int i = 0; i < whereSubList.size(); i++) {
            WhereItem bean = whereSubList.get(i);
            if (i == 0) {
                sb.append(bean.buildSql());
            }
            sb.append(" AND ").append(bean.buildSql());
        }
        return sb.toString();
    }
}
