package org.apache.matrix;

import org.apache.matrix.extension.DataSourceV2Extension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MatrixSqlSessionTest {

    private MatrixSqlSession matrixSqlSession;

    @Before
    public void init(){
        Map<String, Object> ck1Map = new HashMap<>();
        ck1Map.put("url", "127.0.0.1:8123");
        MatrixSqlSession.datasourceConfigMap.put("ck1", ck1Map);
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .config("spark.runSqlDataSourceV2", true)
                .config("enable.push.down.agg", true)
                .withExtensions(new DataSourceV2Extension())
                .getOrCreate();

        matrixSqlSession = new MatrixSqlSession(sparkSession);
    }

    @Test
    public void sqlAlias() {
        String sqlText = "" +
                "select " +
                "   1 + 1 as age," +
                "   'leilei' as name as t;" +
                "select * from t";
        Dataset<Row> dataset = matrixSqlSession.run(sqlText);
        dataset.show();
    }

    @Test
    public void sqlDataSource() {
        String sqlText = "select name, max(age) from `clickhouse.ck1.test`.people where age > 20 group by name as t; " +
                "select * from t limit 5";
        Dataset<Row> dataset = matrixSqlSession.run(sqlText);
        System.out.println(dataset.queryExecution().analyzed().treeString());
        System.out.println(dataset.queryExecution().executedPlan().treeString());
        dataset.show();
    }
}