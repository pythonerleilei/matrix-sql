package org.apache.matrix.parse;

import org.apache.matrix.util.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AliasParse {

    private static final String DELIMITER = ";";
    private static final Pattern SUFFIX_AS_ALIAS_PATTERN = Pattern.compile("[a|A][s|S]\\s+(\\w)+$");

    public static List<AliasSql> listAliasSql(String sqlText){
        List<AliasSql> aliasSqls = new LinkedList<>();
        if(StringUtils.isEmpty(sqlText)){
            return aliasSqls;
        }
        sqlText = sqlText.trim();
        String[] mixSqls = sqlText.split(DELIMITER);
        if(mixSqls == null || mixSqls.length == 0){
            return aliasSqls;
        }
        for(String mixSql : mixSqls){
            if(StringUtils.isEmpty(mixSql)){
                continue;
            }
            AliasSql aliasSql = getAliasSql(mixSql);
            aliasSqls.add(aliasSql);
        }

        return aliasSqls;
    }

    private static AliasSql getAliasSql(String mixSql) {
        AliasSql aliasSql = new AliasSql();
        aliasSql.primarySql = mixSql;
        Matcher matcher = SUFFIX_AS_ALIAS_PATTERN.matcher(mixSql);
        if(matcher.find()){
            String viewName = matcher.group(1);
            String primarySql = matcher.replaceAll("");
            aliasSql.primarySql = primarySql.trim();
            aliasSql.aliasViewName = viewName;
        }
        return aliasSql;
    }

    public static class AliasSql{
        String primarySql;
        String aliasViewName;

        public String getPrimarySql() {
            return primarySql;
        }

        public String getAliasViewName() {
            return aliasViewName;
        }
    }
}
