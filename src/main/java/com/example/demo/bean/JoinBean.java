package com.example.demo.bean;

import com.example.demo.service.BaseParse;
import com.example.demo.service.BaseUtil;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;

import java.util.ArrayList;
import java.util.List;

public class JoinBean extends BaseBean implements BaseParse {
    private List<JoinItem> joinTableList = new ArrayList<>();

    public static void main(String[] args) {
        JoinBean join = new JoinBean();
        join.parseSql(BaseUtil.sql1);
        String s = join.buildSql();

        System.out.println();
    }

    @Override
    public void parseSql(String sql) {
        List<Join> list = BaseUtil.getSelectJoin(sql);
        if (list != null && list.size() > 0) {
            for (Join join : list) {
                JoinItem joinTable = new JoinItem();
                TableBean tableBean = new TableBean();
                List<JoinOn> joinOnList = new ArrayList<>();
                //---------------------------------------------------------
                String joinType = join.getASTNode().jjtGetFirstToken().toString();
                FromItem rightItem = join.getRightItem();
                Expression onExpression = join.getOnExpression();
                if (rightItem != null) {
                    tableBean.parseSql(rightItem.toString());
                }
                if (onExpression != null) {
                    // on t1.id=t2.user_id and t1.age=25 and t2.qq='234533'
                    String onStr = onExpression.toString();
                    if (onStr.toLowerCase().indexOf(" and ") != -1) {
                        String[] splitTemp = onStr.split(" AND|and ");
                        for (String expr : splitTemp) {
                            JoinOn joinOn = new JoinOn();
                            joinOn.parseSql(expr);
                            joinOnList.add(joinOn);
                        }
                    }else{
                        JoinOn joinOn = new JoinOn();
                        joinOn.parseSql(onStr);
                        joinOnList.add(joinOn);
                    }
                }
                joinTable.setJoinType(joinType);
                joinTable.setJoinOnList(joinOnList);
                joinTable.setTableBean(tableBean);
                joinTableList.add(joinTable);
            }
        }
    }

    @Override
    public String buildSql() {
        StringBuffer sb = new StringBuffer();
        for (JoinItem item : joinTableList) {
            StringBuffer sbTemp = new StringBuffer(line);
            String joinType = item.getJoinType();
            String tableBeanStr = item.getTableBean().buildSql();
            List<JoinOn> joinOnList = item.getJoinOnList();
            StringBuffer joinOnSb=new StringBuffer();
            for (int i=0;i<joinOnList.size();i++){
                JoinOn joinOn = joinOnList.get(i);
                if(i==0){
                    joinOnSb.append(" ON ").append(joinOn.buildSql());
                }else{
                    joinOnSb.append(" AND ").append(joinOn.buildSql());
                }
            }
            sbTemp.append(joinType);
            sbTemp.append(tableBeanStr);
            sbTemp.append(joinOnSb.toString());
            sb.append(sbTemp);
        }

        return sb.toString();
    }
}