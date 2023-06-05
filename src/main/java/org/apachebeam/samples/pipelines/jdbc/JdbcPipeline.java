package org.apachebeam.samples.pipelines.jdbc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JdbcPipeline {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pOutput = p.apply(JdbcIO.<String>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver","jdbc:mysql://127.0.0.11/3306/..")
                    .withUsername("root")
                    .withPassword("root")
                )
                .withQuery("SELECT name, city currency from product_info WHERE name = ?")
                .withCoder(StringUtf8Coder.of())
                .withStatementPreparator(new JdbcIO.StatementPreparator() {
                    @Override
                    public void setParameters(PreparedStatement preparedStatement) throws Exception {
                        preparedStatement.setString(1,"iphone");
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<String>() {
                    @Override
                    public String mapRow(ResultSet resultSet) throws Exception {
                        return resultSet.getString(1)+","+resultSet.getString(2)+","+resultSet.getString(3);
                    }
                })
            );

        pOutput.apply(TextIO.write().to("C:\\...jdbc_output").withNumShards(1).withSuffix(".csv"));
        p.run();
    }
}
