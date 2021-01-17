package com.example.demo.service;

public interface BaseParse {
    String line = System.lineSeparator();

    void parseSql(String sql);

    String buildSql();

}
