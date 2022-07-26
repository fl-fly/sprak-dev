package com.fl.spark.core.java.source;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author fl37804
 * @date 2022-06-07
 */
public class ReadClickhouse {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("read clickhouse data");
        // 创建一个spark session 类型的spark对象
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        // 创建数据库配置
        Properties properties = new Properties();
        properties.setProperty("user", "default");
        properties.setProperty("password", "123456");
        properties.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver");

        Dataset<Row> kLineStream = sparkSession.read()
                .jdbc("jdbc:clickhouse://192.168.71.28:28123/level1_stock", "k_line_1day", properties)
                // date_time,hs_security_id,security_id,pre_close_px,open_px,high_px,low_px,close_px,volume,amount
//                .select( "date_time","hs_security_id","security_id","pre_close_px","high_px").limit(500);
                .select( "*").limit(500);

        // 展示数据
        kLineStream.show();
        sparkSession.stop();
    }
}
