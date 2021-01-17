package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import org.apache.commons.lang3.StringUtils;

public class FieldBean implements BaseParse {
    private String tableAlias;
    private String fieldName;
    private String as = "";
    private String fieldAlias = "";

    @Override
    public void parseSql(String subSql) {
        String[] split = null;
        if (subSql.indexOf(" ") != -1) {
            split = subSql.split(" ");

        } else if (subSql.toLowerCase().indexOf(" as ") != -1) {
            split = subSql.split(" as ");
        } else {
            split = new String[]{subSql};
        }

        if (split == null || split.length == 0) {
            return;
        }
        if (split.length == 1) {
            fieldName = split[0].trim();
        } else if (split.length == 2) {
            fieldName = split[0].trim();
            fieldAlias = split[1];
        } else if (split.length == 3) {
            fieldName = split[0].trim();
            as = " AS";
            fieldAlias = split[2].trim();
        }
        if (fieldName.indexOf(".") != -1) {
            String[] temp = fieldName.split("\\.");
            tableAlias = temp[0].trim();
            fieldName = temp[1].trim();
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer();
        if (StringUtils.isBlank(tableAlias)) {
            sb.append(fieldName);
        } else {
            sb.append(tableAlias).append(".").append(fieldName);
        }
        sb.append(as).append(" ").append(fieldAlias);
        return sb.toString();
    }

}
