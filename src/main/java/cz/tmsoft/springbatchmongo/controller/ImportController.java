package cz.tmsoft.springbatchmongo.controller;

import java.util.List;

import cz.tmsoft.springbatchmongo.api.ImportDataRest;
import cz.tmsoft.springbatchmongo.domain.ImportData;
import cz.tmsoft.springbatchmongo.mapper.ImportMapper;
import cz.tmsoft.springbatchmongo.service.ImportService;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@RestController
public class ImportController {

    private ImportService importService;
    private JobLauncher jobLauncher;
    private Job readImportJob;

    public ImportController(final ImportService importService, final JobLauncher jobLauncher, final Job readImportJob) {
        this.importService = importService;
        this.jobLauncher = jobLauncher;
        this.readImportJob = readImportJob;
    }

    @PostMapping("/")
    public void saveImport(@RequestBody List<ImportDataRest> request)
            throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
        List<ImportData> imports = ImportMapper.INSTANCE.map(request);
        importService.saveImport(imports);


        JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();

        JobExecution execution = jobLauncher.run(readImportJob, param);
    }
}
