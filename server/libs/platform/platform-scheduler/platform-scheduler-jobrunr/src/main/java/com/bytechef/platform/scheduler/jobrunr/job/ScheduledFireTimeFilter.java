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

import java.time.Instant;
import java.util.Optional;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.filters.JobServerFilter;
import org.jobrunr.jobs.states.ScheduledState;

/**
 * Makes the moment a run was scheduled for available to the job body, the JobRunr counterpart of Quartz's
 * {@code JobExecutionContext.getFireTime()}. JobRunr calls {@link #onProcessing(Job)} on the worker thread right before
 * the job method, so a thread local carries the value. The first {@link ScheduledState} of a job is the moment its
 * recurring job asked for, also when the run is a retry; a job that was never scheduled reports its creation time.
 *
 * @author Nicholas D'hondt
 */
public class ScheduledFireTimeFilter implements JobServerFilter {

    private static final ThreadLocal<Instant> FIRE_TIME = new ThreadLocal<>();

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
    }

    @Override
    public void onProcessingFailed(Job job, Exception exception) {
        FIRE_TIME.remove();
    }

    @Override
    public void onProcessingSucceeded(Job job) {
        FIRE_TIME.remove();
    }
}
