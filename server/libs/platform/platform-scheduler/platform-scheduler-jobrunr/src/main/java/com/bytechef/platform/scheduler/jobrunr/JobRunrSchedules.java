/*
 * Copyright 2025 ByteChef
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytechef.platform.scheduler.jobrunr;

import static org.jobrunr.jobs.states.StateName.ENQUEUED;
import static org.jobrunr.jobs.states.StateName.PROCESSING;
import static org.jobrunr.jobs.states.StateName.SCHEDULED;
import static org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt;

import com.bytechef.platform.scheduler.jobrunr.job.JobRunrSchedulerJobs;
import com.bytechef.platform.scheduler.jobrunr.util.JobRunrIds;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.lambdas.IocJobLambda;
import org.jobrunr.scheduling.JobBuilder;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.scheduling.RecurringJobBuilder;
import org.jobrunr.storage.ConcurrentJobModificationException;
import org.jobrunr.storage.JobNotFoundException;
import org.jobrunr.storage.StorageProvider;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedule, reschedule and cancel by key on top of JobRunr OSS, with the semantics the Quartz provider has: scheduling
 * an existing key replaces it, cancelling removes what is pending, and a run that is executing finishes.
 *
 * <p>
 * Two JobRunr behaviours make a plain "delete, then create under the same id" unsafe, so this class avoids it:
 * <ul>
 * <li>Creating a job under an id that is still in the table (a deleted job stays there until it is purged) is skipped
 * silently. One-time jobs therefore live in one of two deterministic slots per key: rescheduling removes the other
 * slot's job permanently and uses it, and a job that reschedules its own key while it runs (the webhook and token
 * refresh jobs) lands in the slot it does not occupy.</li>
 * <li>A background job server remembers the last run it created per recurring job id, also after the recurring job is
 * deleted, and catches up every run since then when the id comes back. Each schedule of a recurring key therefore gets
 * a new generation ({@code <base>-g<epoch millis>}), and cancelling deletes every generation of the key together with
 * the run JobRunr already created ahead of time.</li>
 * </ul>
 *
 * @author Nicholas D'hondt
 */
public class JobRunrSchedules {

    static final String GENERATION_SEPARATOR = "-g";

    private static final int SLOT_COUNT = 2;
    private static final int PENDING_RUN_PAGE_SIZE = 1000;

    private static final Logger log = LoggerFactory.getLogger(JobRunrSchedules.class);

    private final JobScheduler jobScheduler;
    private final StorageProvider storageProvider;

    @SuppressFBWarnings("EI")
    public JobRunrSchedules(JobScheduler jobScheduler, StorageProvider storageProvider) {
        this.jobScheduler = jobScheduler;
        this.storageProvider = storageProvider;
    }

    /**
     * Removes the pending one-time job of the key, if any. A job of the key that is executing right now finishes.
     */
    public void cancelOnce(String key) {
        for (int slot = 0; slot < SLOT_COUNT; slot++) {
            UUID jobId = JobRunrIds.jobId(key, slot);
            Job job = findJob(jobId);

            if (job != null && job.getState() != PROCESSING) {
                storageProvider.deletePermanently(jobId);

                log.trace("Removed job {} of key {}", jobId, key);
            }
        }
    }

    /**
     * Deletes every generation of the recurring job and the runs it created ahead of time that have not started.
     */
    public void cancelRecurring(String baseId) {
        String generationPrefix = baseId + GENERATION_SEPARATOR;

        for (RecurringJob recurringJob : storageProvider.getRecurringJobs()) {
            String recurringJobId = recurringJob.getId();

            if (recurringJobId.startsWith(generationPrefix)) {
                jobScheduler.deleteRecurringJob(recurringJobId);

                deletePendingRuns(recurringJobId);

                log.trace("Removed recurring job {}", recurringJobId);
            }
        }
    }

    /**
     * Runs the job once, as soon as a worker is free. Used for the immediate first run of a polling trigger.
     */
    public void enqueue(String jobName, List<String> labels, IocJobLambda<JobRunrSchedulerJobs> jobLambda) {
        jobScheduler.create(
            JobBuilder.aJob()
                .withName(jobName)
                .withLabels(labels)
                .withJobLambda(jobLambda));
    }

    /**
     * Schedules the job at the instant under the key, replacing the pending job of the key.
     */
    public void scheduleOnce(
        String key, Instant executeAt, String jobName, List<String> labels,
        IocJobLambda<JobRunrSchedulerJobs> jobLambda) {

        UUID freeJobId = null;

        for (int slot = 0; slot < SLOT_COUNT; slot++) {
            UUID jobId = JobRunrIds.jobId(key, slot);
            Job job = findJob(jobId);

            if (job != null && job.getState() == PROCESSING) {
                continue;
            }

            if (job != null) {
                storageProvider.deletePermanently(jobId);
            }

            if (freeJobId == null) {
                freeJobId = jobId;
            }
        }

        if (freeJobId == null) {
            throw new IllegalStateException("Both jobs of key " + key + " are executing; cannot schedule another one");
        }

        jobScheduler.create(
            JobBuilder.aJob()
                .withId(freeJobId)
                .withName(jobName)
                .withLabels(labels)
                .scheduleAt(executeAt)
                .withJobLambda(jobLambda));

        log.trace("Scheduled job {} of key {} at {}", freeJobId, key, executeAt);
    }

    /**
     * Replaces the recurring job of the base id with a new generation built from the given builder.
     *
     * @return the id of the new generation
     */
    public String scheduleRecurring(String baseId, RecurringJobBuilder recurringJobBuilder) {
        cancelRecurring(baseId);

        String recurringJobId = baseId + GENERATION_SEPARATOR + System.currentTimeMillis();

        return jobScheduler.createRecurrently(recurringJobBuilder.withId(recurringJobId));
    }

    private void deletePendingRuns(String recurringJobId) {
        Instant latestScheduledAt = storageProvider.getRecurringJobLatestScheduledInstant(
            recurringJobId, SCHEDULED, ENQUEUED);

        if (latestScheduledAt == null) {
            return;
        }

        List<Job> pendingJobs = new ArrayList<>(
            storageProvider.getScheduledJobs(latestScheduledAt.plusMillis(1), ascOnUpdatedAt(PENDING_RUN_PAGE_SIZE)));

        pendingJobs.addAll(storageProvider.getJobList(ENQUEUED, ascOnUpdatedAt(PENDING_RUN_PAGE_SIZE)));

        for (Job pendingJob : pendingJobs) {
            String pendingRecurringJobId = pendingJob.getRecurringJobId()
                .orElse(null);

            if (Objects.equals(pendingRecurringJobId, recurringJobId)) {
                try {
                    jobScheduler.delete(pendingJob.getId(), "Trigger cancelled");
                } catch (ConcurrentJobModificationException concurrentJobModificationException) {
                    log.debug(
                        "Run {} of recurring job {} changed state while it was being cancelled", pendingJob.getId(),
                        recurringJobId);
                }
            }
        }
    }

    private @Nullable Job findJob(UUID jobId) {
        try {
            return storageProvider.getJobById(jobId);
        } catch (JobNotFoundException jobNotFoundException) {
            return null;
        }
    }
}
