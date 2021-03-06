package cz.tmsoft.springbatchmongo.dao;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

/**
 * @author tomas.marianek
 * @since 22.09.2021
 */
@Repository
public class MongoJobExecutionDao extends AbstractMongoDao implements JobExecutionDao {

    @PostConstruct
    public void init() {
        super.init();
        getCollection().createIndex(
                (Bson) BasicDBObjectBuilder.start()
                        .add(JOB_EXECUTION_ID_KEY, 1)
                        .add(JOB_INSTANCE_ID_KEY, 1)
                        .get()
        );
    }

    public void saveJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);
        jobExecution.incrementVersion();
        Long id = getNextId(JobExecution.class.getSimpleName(), mongoTemplate);
        save(jobExecution, id);
    }

    private void save(JobExecution jobExecution, Long id) {
        jobExecution.setId(id);
        Document object = toDbObjectWithoutVersion(jobExecution);
        object.put(VERSION_KEY, jobExecution.getVersion());
        getCollection().insertOne(object);
    }

    private Document toDbObjectWithoutVersion(JobExecution jobExecution) {
        Document retVal = new Document();

        retVal.put(JOB_EXECUTION_ID_KEY, jobExecution.getId());
        retVal.put(JOB_INSTANCE_ID_KEY, jobExecution.getJobId());
        retVal.put(START_TIME_KEY, jobExecution.getStartTime());
        retVal.put(END_TIME_KEY, jobExecution.getEndTime());
        retVal.put(STATUS_KEY, jobExecution.getStatus().toString());
        retVal.put(EXIT_CODE_KEY, jobExecution.getExitStatus().getExitCode());
        retVal.put(EXIT_MESSAGE_KEY, jobExecution.getExitStatus().getExitDescription());
        retVal.put(CREATE_TIME_KEY, jobExecution.getCreateTime());
        retVal.put(LAST_UPDATED_KEY, jobExecution.getLastUpdated());
        return retVal;
    }

    private void validateJobExecution(JobExecution jobExecution) {
        Assert.notNull(jobExecution, "JobExecution cannot be null.");
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    public synchronized void updateJobExecution(JobExecution jobExecution) {
        validateJobExecution(jobExecution);

        Long jobExecutionId = jobExecution.getId();
        Assert.notNull(jobExecutionId, "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");
        Assert.notNull(jobExecution.getVersion(), "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

        Integer version = jobExecution.getVersion() + 1;

        if (getCollection().find(jobExecutionIdObj(jobExecutionId)) == null) {
            throw new NoSuchObjectException(String.format("Invalid JobExecution, ID %s not found.", jobExecutionId));
        }

        Document object = toDbObjectWithoutVersion(jobExecution);
        object.put(VERSION_KEY, version);

        replace (getCollection(), (Bson) BasicDBObjectBuilder.start()
                .add(JOB_EXECUTION_ID_KEY, jobExecutionId)
                .add(VERSION_KEY, jobExecution.getVersion())
                .get(), (Bson) object, false, false);
       /* WriteResult update = getCollection().update(
                BasicDBObjectBuilder.start()
                        .add(JOB_EXECUTION_ID_KEY, jobExecutionId)
                        .add(VERSION_KEY, jobExecution.getVersion())
                        .get(),
                object
        );*/

        jobExecution.incrementVersion();
    }

    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        Assert.notNull(jobInstance, "Job cannot be null.");
        Long id = jobInstance.getId();
        Assert.notNull(id, "Job Id cannot be null.");
        MongoCursor dbCursor = getCollection().find(jobInstanceIdObj(id)).sort(new BasicDBObject(JOB_EXECUTION_ID_KEY, -1)).cursor();
        List<JobExecution> result = new ArrayList<>();
        while (dbCursor.hasNext()) {
            Document dbObject = (Document) dbCursor.next();
            result.add(mapJobExecution(jobInstance, dbObject));
        }
        return result;
    }

    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        Long id = jobInstance.getId();

        MongoCursor dbCursor = getCollection().find(jobInstanceIdObj(id)).sort(new BasicDBObject(CREATE_TIME_KEY, -1)).limit(1).cursor();
        if (!dbCursor.hasNext()) {
            return null;
        } else {
            Document singleResult = (Document) dbCursor.next();
            if (dbCursor.hasNext()) {
                throw new IllegalStateException("There must be at most one latest job execution");
            }
            return mapJobExecution(jobInstance, singleResult);
        }
    }

    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        DBCursor instancesCursor = ((DBCollection) mongoTemplate.getCollection(JobInstance.class.getSimpleName()))
                .find(new BasicDBObject(JOB_NAME_KEY, jobName), jobInstanceIdObj(1L));
        List<Long> ids = new ArrayList<>();
        while (instancesCursor.hasNext()) {
            ids.add((Long) instancesCursor.next().get(JOB_INSTANCE_ID_KEY));
        }

        MongoCursor dbCursor = getCollection().find(
                (ClientSession) BasicDBObjectBuilder
                        .start()
                        .add(JOB_INSTANCE_ID_KEY, new BasicDBObject("$in", ids.toArray()))
                        .add(END_TIME_KEY, null).get()).sort(
                jobExecutionIdObj(-1L)).cursor();
        Set<JobExecution> result = new HashSet<>();
        while (dbCursor.hasNext()) {
            result.add(mapJobExecution((Document) dbCursor.next()));
        }
        return result;
    }

    public JobExecution getJobExecution(Long executionId) {
        return mapJobExecution((Document) getCollection().find(jobExecutionIdObj(executionId)).first());
    }

    public void synchronizeStatus(JobExecution jobExecution) {
        Long id = jobExecution.getId();
        Document jobExecutionObject = (Document) getCollection().find(jobExecutionIdObj(id)).first();
        int currentVersion = jobExecutionObject != null ? ((Integer) jobExecutionObject.get(VERSION_KEY)) : 0;
        if (currentVersion != jobExecution.getVersion()) {
            if (jobExecutionObject == null) {
                save(jobExecution, id);
                jobExecutionObject = (Document) getCollection().find(jobExecutionIdObj(id)).first();
            }
            String status = (String) jobExecutionObject.get(STATUS_KEY);
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(currentVersion);
        }
    }

    @Override
    protected MongoCollection getCollection() {
        return mongoTemplate.getCollection(JobExecution.class.getSimpleName());
    }

    private JobExecution mapJobExecution(Document dbObject) {
        return mapJobExecution(null, dbObject);
    }

    private JobExecution mapJobExecution(JobInstance jobInstance, Document dbObject) {
        if (dbObject == null) {
            return null;
        }
        Long id = (Long) dbObject.get(JOB_EXECUTION_ID_KEY);
        JobExecution jobExecution;

        if (jobInstance == null) {
            jobExecution = new JobExecution(id);
        } else {
            JobParameters jobParameters = getJobParameters(jobInstance.getId(), mongoTemplate);
            jobExecution = new JobExecution(jobInstance, id, jobParameters, null);
        }
        jobExecution.setStartTime((Date) dbObject.get(START_TIME_KEY));
        jobExecution.setEndTime((Date) dbObject.get(END_TIME_KEY));
        jobExecution.setStatus(BatchStatus.valueOf((String) dbObject.get(STATUS_KEY)));
        jobExecution.setExitStatus(new ExitStatus(((String) dbObject.get(EXIT_CODE_KEY)), (String) dbObject.get(EXIT_MESSAGE_KEY)));
        jobExecution.setCreateTime((Date) dbObject.get(CREATE_TIME_KEY));
        jobExecution.setLastUpdated((Date) dbObject.get(LAST_UPDATED_KEY));
        jobExecution.setVersion((Integer) dbObject.get(VERSION_KEY));

        return jobExecution;
    }
}