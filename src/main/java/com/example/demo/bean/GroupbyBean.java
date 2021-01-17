package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import com.example.demo.service.BaseUtil;
import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;
import java.util.List;


public class GroupbyBean extends BaseBean implements BaseParse {
    private List<FieldBean> fieldBeanList = new ArrayList<>();

    public static void main(String[] args) {
        GroupbyBean bean = new GroupbyBean();
        bean.parseSql(BaseUtil.sql1);
        String s = bean.buildSql();
        System.out.println(s);
    }

    @Override
    public void parseSql(String sql) {
        List<Expression> list = BaseUtil.getSelectGroupby(sql);
        for (Expression item : list) {
            FieldBean fieldBean = new FieldBean();
            fieldBean.parseSql(item.toString());
            fieldBeanList.add(fieldBean);
        }
        System.out.println();
    }

    @Override
    public String buildSql() {
        StringBuffer sb=new StringBuffer(line);
        sb.append("GROUP BY ");
        for (FieldBean bean:fieldBeanList){
            sb.append(bean.buildSql()).append(",");
        }
        String subSql=sb.toString();
        if(subSql.endsWith(",")){
            subSql=subSql.substring(0,subSql.length()-1);
        }
        return subSql;
    }
}
