package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import com.example.demo.service.BaseUtil;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class SelectBean implements BaseParse {
    //1.普通字段
    private List<FieldBean> fieldBeanList = new ArrayList<>();
    //2.子查询字段
    private List<SqlBean> sqlBeanList = new ArrayList<>();

    public static void main(String[] args) {
        SelectBean selectBean = new SelectBean();
        selectBean.parseSql(BaseUtil.sql1);
    }

    @Override
    public void parseSql(String sql) {
        List<SelectItem> selectItems = BaseUtil.getSelectItems(sql);
        for (SelectItem select : selectItems) {
            String subSql = select.toString().trim();
            //1.子查询
            if (subSql.toLowerCase().startsWith("(select")) {
                subSql = subSql.substring(1, subSql.lastIndexOf(")"));
                SqlBean sqlBean =new SqlBean();
                sqlBean.parseSql(subSql);
                sqlBeanList.add(sqlBean);
            }
            //2.普通字段
            else {
                FieldBean fieldBean = new FieldBean();
                fieldBean.parseSql(subSql);
                fieldBeanList.add(fieldBean);
            }
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer("SELECT");
        for (FieldBean bean:fieldBeanList){
            sb.append(bean.buildSql()).append(",");
        }
        for (SqlBean bean:sqlBeanList){
            sb.append(bean.buildSql()).append(",");
        }
        return sb.toString();
    }
}
