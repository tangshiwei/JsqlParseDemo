package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import com.example.demo.service.BaseUtil;
import net.sf.jsqlparser.statement.select.Limit;

public class LimitBean extends BaseBean implements BaseParse {
    private String offset;
    private String rowCount;

    public static void main(String[] args) {
        LimitBean bean = new LimitBean();
        bean.parseSql(BaseUtil.sql1);

        String s = bean.buildSql();
        System.out.println(s);
    }

    @Override
    public void parseSql(String sql) {
        Limit limit = BaseUtil.getLimit(sql);
        if (limit == null) {
            this.sql = "";
            return;
        }
        this.sql = limit.toString();
        if (limit.getOffset() != null) {
            this.offset = limit.getOffset().toString();
        }
        if (limit.getRowCount() != null) {
            this.rowCount = limit.getRowCount().toString();
        }

    }

    @Override
    public String buildSql() {
        return this.sql;
    }
}
