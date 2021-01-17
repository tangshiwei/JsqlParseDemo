package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import org.apache.commons.lang3.StringUtils;

public class TableBean extends BaseBean implements BaseParse {
    private String tableAlias;
    private String tableName;

    @Override
    public void parseSql(String subSql) {
        this.sql = subSql;
        String[] split = null;
        if (subSql.indexOf(" ") != -1) {
            split = subSql.split(" ");
        } else {
            split = new String[]{subSql};
        }
        if (split == null || split.length == 0) {
            return;
        }
        if (split.length == 1) {
            tableName = split[0];
        } else if (split.length == 2) {
            tableName = split[0];
            tableAlias = split[1];
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer();
        if (StringUtils.isNotBlank(tableAlias)) {
            sb.append(tableName).append(" ").append(tableAlias);
        } else {
            sb.append(tableName);
        }

        return sb.toString();
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
