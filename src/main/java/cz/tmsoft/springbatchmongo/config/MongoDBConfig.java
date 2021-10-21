package cz.tmsoft.springbatchmongo.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;

/**
 * @author tomas.marianek
 * @since 20.09.2021
 */
@Configuration
public class MongoDBConfig extends AbstractMongoClientConfiguration {

    private static Logger logger = LoggerFactory.getLogger(MongoDBConfig.class);

    @Value("${spring.data.mongodb.database}")
    private String database;

    @Value("${spring.data.mongodb.host}")
    private String host;

    @Override
    protected String getDatabaseName() {
        return database;
    }

    @Override
    public void configureClientSettings(MongoClientSettings.Builder builder) {
        builder.applyConnectionString(new ConnectionString(host));
    }
}
