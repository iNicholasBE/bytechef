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

package com.bytechef.platform.scheduler.jobrunr.job;

import java.time.Duration;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.filters.RetryFilter;
import org.jobrunr.jobs.states.FailedState;

/**
 * Replaces JobRunr's default retry filter on the background job server. Every job keeps JobRunr's default policy (10
 * retries, 3^n seconds apart) except the one-time task that resumes a suspended job
 * ({@link JobRunrSchedulerJobs#executeOneTimeTask}), which retries with the delays of the Quartz
 * {@code OneTimeSchedulerJob}: 2 seconds, doubling, at most 5 minutes, 10 retries.
 *
 * @author Nicholas D'hondt
 */
public class OneTimeTaskRetryFilter extends RetryFilter {

    static final int MAX_RETRY_ATTEMPTS = 10;
    static final String ONE_TIME_TASK_METHOD_NAME = "executeOneTimeTask";

    private static final Duration INITIAL_RETRY_DELAY = Duration.ofSeconds(2);
    private static final Duration MAX_RETRY_DELAY = Duration.ofMinutes(5);

    public OneTimeTaskRetryFilter() {
        super(MAX_RETRY_ATTEMPTS);
    }

    static Duration getRetryDelay(int retryAttempt) {
        Duration retryDelay = INITIAL_RETRY_DELAY.multipliedBy(1L << Math.min(retryAttempt, 16));

        return retryDelay.compareTo(MAX_RETRY_DELAY) > 0 ? MAX_RETRY_DELAY : retryDelay;
    }

    @Override
    protected long getSecondsToAdd(Job job) {
        if (!isOneTimeTask(job)) {
            return super.getSecondsToAdd(job);
        }

        long failureCount = job.getJobStatesOfType(FailedState.class)
            .count();

        Duration retryDelay = getRetryDelay((int) Math.max(failureCount - 1, 0));

        return retryDelay.toSeconds();
    }

    private static boolean isOneTimeTask(Job job) {
        JobDetails jobDetails = job.getJobDetails();

        return JobRunrSchedulerJobs.class.getName()
            .equals(jobDetails.getClassName()) && ONE_TIME_TASK_METHOD_NAME.equals(jobDetails.getMethodName());
    }
}
