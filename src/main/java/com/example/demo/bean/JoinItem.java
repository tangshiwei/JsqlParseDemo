package com.example.demo.bean;

import java.util.ArrayList;
import java.util.List;

public class JoinItem {
    private String joinType;
    private TableBean tableBean = new TableBean();
    private List<JoinOn> joinOnList = new ArrayList<>();

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType.toUpperCase()+" JOIN ";
    }

    public TableBean getTableBean() {
        return tableBean;
    }

    public void setTableBean(TableBean tableBean) {
        this.tableBean = tableBean;
    }

    public List<JoinOn> getJoinOnList() {
        return joinOnList;
    }

    public void setJoinOnList(List<JoinOn> joinOnList) {
        this.joinOnList = joinOnList;
    }
}
