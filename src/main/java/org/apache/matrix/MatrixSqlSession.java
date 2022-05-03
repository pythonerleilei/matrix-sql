package org.apache.matrix;

import org.apache.matrix.parse.AliasParse;
import org.apache.matrix.parse.AliasParse.AliasSql;
import org.apache.matrix.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class MatrixSqlSession {

    private SparkSession sparkSession;

    public MatrixSqlSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public Dataset<Row> run(String sqlText){
        if(StringUtils.isEmpty(sqlText)){
            throw new RuntimeException("input sql is empty!");
        }
        List<AliasSql> aliasSqls = AliasParse.listAliasSql(sqlText);
        Dataset<Row> dataset = null;
        for(AliasSql aliasSql : aliasSqls){
            if(StringUtils.isEmpty(aliasSql.getPrimarySql())){
                continue;
            }
            dataset = sparkSession.sql(aliasSql.getPrimarySql());
            if(!StringUtils.isEmpty(aliasSql.getAliasViewName())){
                dataset.createOrReplaceTempView(aliasSql.getAliasViewName());
            }
        }
        return dataset;
    }

}
