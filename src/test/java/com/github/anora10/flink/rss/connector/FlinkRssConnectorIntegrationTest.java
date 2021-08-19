package com.github.anora10.flink.rss.connector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

public class FlinkRssConnectorIntegrationTest {

  @Test
  public void testFlinkRssConnectorWithLimitedNumberOfRows() throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(8);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(
        "CREATE TEMPORARY TABLE rss ( \n"
            + "  `title` STRING,\n"
            + "  `description` STRING \n"
            + ") WITH (\n"
            + "  'connector' = 'rss-connector', \n"
            + "  'uri' = " +
                "'https://rss.nytimes.com/services/xml/rss/nyt/Health.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Europe.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Dance.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Jobs.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Movies.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Music.xml, " +
                "https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml, " +
                "https://index.hu/24ora/rss'" +
                ",\n"
            + "  'refresh-interval' = '5000',\n"
            + "  'format' = 'xml' \n"
            + ")");

    Table tableResult = tableEnv.sqlQuery("SELECT * FROM rss");

    DataStream<Row> rowDataStream = tableEnv.toDataStream(tableResult);
    rowDataStream.print();
    env.execute("RSS connector example Job");
  }
}
