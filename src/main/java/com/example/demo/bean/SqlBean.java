package com.example.demo.bean;

import com.example.demo.service.BaseParse;

public class SqlBean extends BaseBean implements BaseParse {
    private SelectBean selectBean = new SelectBean();
    private FromBean fromBean = new FromBean();
    private WhereBean whereBean = new WhereBean();
    private JoinBean joinBean = new JoinBean();
    private GroupbyBean groupbyBean = new GroupbyBean();
    private OrderbyBean orderbyBean = new OrderbyBean();
    private LimitBean limitBean = new LimitBean();
    @Override
    public void parseSql(String sql) {

    }

    @Override
    public String buildSql() {
        StringBuffer sb=new StringBuffer();
        sb.append(selectBean.buildSql());
        sb.append(fromBean.buildSql());
        sb.append(joinBean.buildSql());
        sb.append(whereBean.buildSql());
        sb.append(groupbyBean.buildSql());
        sb.append(orderbyBean.buildSql());
        sb.append(limitBean.buildSql());
        return sb.toString();
    }
}
