package com.assistant.pos.migrator;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@SuppressWarnings("WeakerAccess")
public class ConnUtils {

    public JdbcTemplate getJdbcTemplate(String dbUrl) {
        return new JdbcTemplate(getSQLiteDataSource(dbUrl));
    }

    private DriverManagerDataSource getSQLiteDataSource(String dbUrl) {
        String url = "jdbc:sqlite:" + dbUrl;

        DriverManagerDataSource dSource = new DriverManagerDataSource();
        dSource.setDriverClassName("org.sqlite.JDBC");
        dSource.setUrl(url);

        return dSource;
    }
}
