package org.apache.matrix;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MatrixSqlSessionTest {

    private MatrixSqlSession matrixSqlSession;

    @Before
    public void init(){
        SparkSession sparkSession = SparkSession.builder().master("local[1]").getOrCreate();
        matrixSqlSession = new MatrixSqlSession(sparkSession);
    }

    @Test
    public void sql() {
        String sqlText = "" +
                "select " +
                "   1 + 1 as age," +
                "   'leilei' as name as t;" +
                "select * from t";
        Dataset<Row> dataset = matrixSqlSession.run(sqlText);
        dataset.show();
    }
}