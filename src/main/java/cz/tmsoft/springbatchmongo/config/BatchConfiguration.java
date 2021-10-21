package cz.tmsoft.springbatchmongo.config;

import cz.tmsoft.springbatchmongo.domain.ImportData;
import cz.tmsoft.springbatchmongo.service.ImportDataProccesor;
import cz.tmsoft.springbatchmongo.service.ImportDataReade;
import cz.tmsoft.springbatchmongo.service.ImportDataWrite;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author tomas.marianek
 * @since 21.09.2021
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;


    @Bean
    public Job c(){
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<ImportData, ImportData> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }


    @Bean
    public ImportDataReade reader(){
        return new ImportDataReade();
    }

    @Bean
    public ImportDataProccesor processor(){
        return new ImportDataProccesor();
    }

    @Bean
    public ImportDataWrite writer(){
        return new ImportDataWrite();
    }
}
