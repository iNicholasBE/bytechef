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
import java.time.Instant;
import java.util.Optional;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.filters.JobServerFilter;
import org.jobrunr.jobs.states.ScheduledState;
import org.jspecify.annotations.Nullable;

/**
 * Makes the moment a run was scheduled for available to the job body, the JobRunr counterpart of Quartz's
 * {@code JobExecutionContext.getFireTime()}. JobRunr calls {@link #onProcessing(Job)} on the worker thread right before
 * the job method, so a thread local carries the value. The first {@link ScheduledState} of a job is the moment its
 * recurring job asked for, also when the run is a retry; a job that was never scheduled reports its creation time.
 *
 * <p>
 * JobRunr OSS enqueues a scheduled job when it falls due within the next poll interval, and a worker can start it
 * before that moment. {@link #awaitScheduledTime()} lets a job body wait for the latest {@link ScheduledState}, so a
 * trigger never fires and a suspended job never resumes before its time.
 *
 * @author Nicholas D'hondt
 */
public class ScheduledFireTimeFilter implements JobServerFilter {

    private static final ThreadLocal<Instant> FIRE_TIME = new ThreadLocal<>();
    private static final ThreadLocal<Instant> NOT_BEFORE = new ThreadLocal<>();
    private static final ThreadLocal<String> RECURRING_JOB_ID = new ThreadLocal<>();

    /**
     * @return the id of the recurring job that created the run on this thread, or {@code null}
     */
    public static @Nullable String getRecurringJobId() {
        return RECURRING_JOB_ID.get();
    }

    /**
     * Sleeps until the moment the run was scheduled for, when a worker picked it up early. Returns at once outside a
     * JobRunr job and for jobs that were never scheduled.
     */
    public static void awaitScheduledTime() throws InterruptedException {
        Instant notBefore = NOT_BEFORE.get();

        if (notBefore == null) {
            return;
        }

        Duration remaining = Duration.between(Instant.now(), notBefore);

        if (remaining.isPositive()) {
            Thread.sleep(remaining);
        }
    }

    /**
     * @return the scheduled moment of the run on this thread, or now when called outside a JobRunr job
     */
    public static Instant getFireTime() {
        return Optional.ofNullable(FIRE_TIME.get())
            .orElseGet(Instant::now);
    }

    @Override
    public void onProcessing(Job job) {
        Instant fireTime = job.getJobStatesOfType(ScheduledState.class)
            .findFirst()
            .map(ScheduledState::getScheduledAt)
            .orElseGet(job::getCreatedAt);

        FIRE_TIME.set(fireTime);

        job.getLastJobStateOfType(ScheduledState.class)
            .map(ScheduledState::getScheduledAt)
            .ifPresentOrElse(NOT_BEFORE::set, NOT_BEFORE::remove);

        job.getRecurringJobId()
            .ifPresentOrElse(RECURRING_JOB_ID::set, RECURRING_JOB_ID::remove);
    }

    @Override
    public void onProcessingFailed(Job job, Exception exception) {
        FIRE_TIME.remove();
        NOT_BEFORE.remove();
        RECURRING_JOB_ID.remove();
    }

    @Override
    public void onProcessingSucceeded(Job job) {
        FIRE_TIME.remove();
        NOT_BEFORE.remove();
        RECURRING_JOB_ID.remove();
    }
}
