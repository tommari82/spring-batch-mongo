package cz.tmsoft.springbatchmongo.dao;


import java.util.Collection;
import java.util.Date;

import javax.annotation.PostConstruct;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Repository
public class MongoStepExecutionDao extends AbstractMongoDao implements StepExecutionDao {

    @PostConstruct
    public void init() {
        super.init();
        getCollection().createIndex((Bson) BasicDBObjectBuilder.start().add(STEP_EXECUTION_ID_KEY, 1).add(JOB_EXECUTION_ID_KEY, 1).get());

    }

    public void saveStepExecution(StepExecution stepExecution) {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");

        validateStepExecution(stepExecution);

        stepExecution.setId(getNextId(StepExecution.class.getSimpleName(), mongoTemplate));
        stepExecution.incrementVersion(); // should be 0 now
        Document object = toDbObjectWithoutVersion(stepExecution);
        object.put(VERSION_KEY, stepExecution.getVersion());
        getCollection().insertOne(object);

    }

    private Document toDbObjectWithoutVersion(StepExecution stepExecution) {
        Document retVal = new Document();
        retVal.put(STEP_EXECUTION_ID_KEY, stepExecution.getId());

        retVal.put(STEP_NAME_KEY, stepExecution.getStepName());
        retVal.put(JOB_EXECUTION_ID_KEY, stepExecution.getJobExecutionId());
        retVal.put(START_TIME_KEY, stepExecution.getStartTime());
        retVal.put(END_TIME_KEY, stepExecution.getEndTime());
        retVal.put(STATUS_KEY, stepExecution.getStatus().toString());
        retVal.put(COMMIT_COUNT_KEY, stepExecution.getCommitCount());
        retVal.put(READ_COUNT_KEY, stepExecution.getReadCount());
        retVal.put(FILTER_COUT_KEY, stepExecution.getFilterCount());
        retVal.put(WRITE_COUNT_KEY, stepExecution.getWriteCount());
        retVal.put(EXIT_CODE_KEY, stepExecution.getExitStatus().getExitCode());
        retVal.put(EXIT_MESSAGE_KEY, stepExecution.getExitStatus().getExitDescription());
        retVal.put(READ_SKIP_COUNT_KEY, stepExecution.getReadSkipCount());
        retVal.put(WRITE_SKIP_COUNT_KEY, stepExecution.getWriteSkipCount());
        retVal.put(PROCESS_SKIP_COUT_KEY, stepExecution.getProcessSkipCount());
        retVal.put(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount());
        retVal.put(LAST_UPDATED_KEY, stepExecution.getLastUpdated());

        return retVal;
    }

    public synchronized void updateStepExecution(StepExecution stepExecution) {
        // Attempt to prevent concurrent modification errors by blocking here if
        // someone is already trying to do it.
        Integer currentVersion = stepExecution.getVersion();
        Integer newVersion = currentVersion + 1;
        Document object = toDbObjectWithoutVersion(stepExecution);
        object.put(VERSION_KEY, newVersion);

        Document filter = new Document(STEP_EXECUTION_ID_KEY, stepExecution.getId());
        filter.put(VERSION_KEY, currentVersion);

        getCollection().replaceOne(filter, object);

        /*// Avoid concurrent modifications...
        DBObject lastError = mongoTemplate.getDb().getLastError();
        if (!((Boolean) lastError.get(UPDATED_EXISTING_STATUS))) {
            logger.error("Update returned status {}", lastError);
            DBObject existingStepExecution = getCollection().findOne(stepExecutionIdObj(stepExecution.getId()), new BasicDBObject(VERSION_KEY, 1));
            if (existingStepExecution == null) {
                throw new IllegalArgumentException("Can't update this stepExecution, it was never saved.");
            }
            Integer curentVersion = ((Integer) existingStepExecution.get(VERSION_KEY));
            throw new OptimisticLockingFailureException("Attempt to update job execution id="
                    + stepExecution.getId() + " with wrong version (" + currentVersion
                    + "), where current version is " + curentVersion);
        }*/

        stepExecution.incrementVersion();
    }


    static BasicDBObject stepExecutionIdObj(Long id) {
        return new BasicDBObject(STEP_EXECUTION_ID_KEY, id);
    }


    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        return mapStepExecution((Document) getCollection().find((Bson) BasicDBObjectBuilder.start()
                .add(STEP_EXECUTION_ID_KEY, stepExecutionId)
                .add(JOB_EXECUTION_ID_KEY, jobExecution.getId()).get()).first(), jobExecution);
    }

    private StepExecution mapStepExecution(Document object, JobExecution jobExecution) {
        if (object == null) {
            return null;
        }
        StepExecution stepExecution = new StepExecution((String) object.get(STEP_NAME_KEY), jobExecution, ((Long) object.get(STEP_EXECUTION_ID_KEY)));
        stepExecution.setStartTime((Date) object.get(START_TIME_KEY));
        stepExecution.setEndTime((Date) object.get(END_TIME_KEY));
        stepExecution.setStatus(BatchStatus.valueOf((String) object.get(STATUS_KEY)));
        stepExecution.setCommitCount((Integer) object.get(COMMIT_COUNT_KEY));
        stepExecution.setReadCount((Integer) object.get(READ_COUNT_KEY));
        stepExecution.setFilterCount((Integer) object.get(FILTER_COUT_KEY));
        stepExecution.setWriteCount((Integer) object.get(WRITE_COUNT_KEY));
        stepExecution.setExitStatus(new ExitStatus((String) object.get(EXIT_CODE_KEY), ((String) object.get(EXIT_MESSAGE_KEY))));
        stepExecution.setReadSkipCount((Integer) object.get(READ_SKIP_COUNT_KEY));
        stepExecution.setWriteSkipCount((Integer) object.get(WRITE_SKIP_COUNT_KEY));
        stepExecution.setProcessSkipCount((Integer) object.get(PROCESS_SKIP_COUT_KEY));
        stepExecution.setRollbackCount((Integer) object.get(ROLLBACK_COUNT_KEY));
        stepExecution.setLastUpdated((Date) object.get(LAST_UPDATED_KEY));
        stepExecution.setVersion((Integer) object.get(VERSION_KEY));
        return stepExecution;

    }

    public void addStepExecutions(JobExecution jobExecution) {
        MongoCursor stepsCoursor = getCollection().find(jobExecutionIdObj(jobExecution.getId())).sort(stepExecutionIdObj(1L)).cursor();
        while (stepsCoursor.hasNext()) {
            Document stepObject = (Document) stepsCoursor.next();
            //Calls constructor of StepExecution, which adds the step; Wow, that's unclear code!
            mapStepExecution(stepObject, jobExecution);
        }
    }

    @Override
    protected MongoCollection getCollection() {
        return mongoTemplate.getCollection(StepExecution.class.getSimpleName());
    }

    private void validateStepExecution(StepExecution stepExecution) {
        Assert.notNull(stepExecution, "StepExecution cannot be null.");
        Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

    @Override
    public void saveStepExecutions(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save an null collect of step executions");
        for (StepExecution stepExecution : stepExecutions) {
            saveStepExecution(stepExecution);
        }

    }

    @Override
    public StepExecution getLastStepExecution(final JobInstance jobInstance, final String stepName) {
        Document filter = new Document(JOB_EXECUTION_ID_KEY, jobInstance.getInstanceId());
        filter.put(STEP_NAME_KEY, stepName);
        StepExecution first = (StepExecution) getCollection().find(filter).sort(new Document(START_TIME_KEY, 1)).sort(new Document(STEP_NAME_KEY, 1)).first();

        return first;
    }

    @Override
    public int countStepExecutions(final JobInstance jobInstance, final String stepName) {
        Document filter = new Document(JOB_EXECUTION_ID_KEY, jobInstance.getInstanceId());
        filter.put(STEP_NAME_KEY, stepName);
        MongoCursor cursor = getCollection().find(filter).cursor();
        if (!cursor.hasNext()) {
            return 0;
        } else {
            int i = 0;
            while (cursor.hasNext()) {
                i += 1;
            }
            return i;

        }
    }
}