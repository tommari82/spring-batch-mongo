package cz.tmsoft.springbatchmongo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class SpringmongobatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringmongobatchApplication.class, args);
	}

}
