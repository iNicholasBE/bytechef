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

import com.bytechef.platform.scheduler.jobrunr.job.JobRunrSchedulerJobs;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.ZoneOffset;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.scheduling.RecurringJobBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

/**
 * Registers the hourly Stripe usage report when billing is enabled, the JobRunr counterpart of
 * {@code BillingSchedulingConfiguration}. The id is fixed and the schedule never changes, so registering it on every
 * start updates the same recurring job.
 *
 * @author Nicholas D'hondt
 */
public class JobRunrStripeUsageReportScheduler {

    static final String STRIPE_USAGE_REPORT = "stripe-usage-report";

    private final JobScheduler jobScheduler;

    @SuppressFBWarnings("EI")
    public JobRunrStripeUsageReportScheduler(JobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void scheduleStripeUsageReport() {
        jobScheduler.createRecurrently(
            RecurringJobBuilder.aRecurringJob()
                .withId(STRIPE_USAGE_REPORT)
                .withName("Stripe usage report")
                .withCron("0 0 * * * *")
                .withZoneId(ZoneOffset.UTC)
                .withJobLambda((JobRunrSchedulerJobs jobs) -> jobs.executeStripeUsageReport()));
    }
}
