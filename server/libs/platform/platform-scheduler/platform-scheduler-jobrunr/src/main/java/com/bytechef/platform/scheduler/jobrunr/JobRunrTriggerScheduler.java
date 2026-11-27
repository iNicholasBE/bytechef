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

import com.bytechef.commons.util.JsonUtils;
import com.bytechef.config.ApplicationProperties;
import com.bytechef.platform.scheduler.TriggerScheduler;
import com.bytechef.platform.scheduler.jobrunr.job.JobRunrSchedulerJobs;
import com.bytechef.platform.scheduler.jobrunr.util.JobRunrIds;
import com.bytechef.platform.scheduler.jobrunr.util.QuartzCronExpressionConverter;
import com.bytechef.platform.workflow.WorkflowExecutionId;
import com.bytechef.tenant.TenantContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import org.jobrunr.scheduling.RecurringJobBuilder;

/**
 * {@link TriggerScheduler} on JobRunr OSS. Schedule and polling triggers are recurring jobs, one-time tasks and dynamic
 * webhook refreshes are jobs scheduled at an instant. The job bodies are in
 * {@link com.bytechef.platform.scheduler.jobrunr.job.JobRunrSchedulerJobs}.
 *
 * @author Nicholas D'hondt
 */
public class JobRunrTriggerScheduler implements TriggerScheduler {

    static final String DYNAMIC_WEBHOOK_TRIGGER_REFRESH = "dynamic-webhook-trigger-refresh";
    static final String ONE_TIME_TASK = "one-time-task";
    static final String POLLING_TRIGGER = "polling-trigger";
    static final String SCHEDULE_TRIGGER = "schedule-trigger";

    private final JobRunrSchedules jobRunrSchedules;
    private final Duration pollingTriggerCheckPeriod;

    @SuppressFBWarnings("EI")
    public JobRunrTriggerScheduler(
        ApplicationProperties.Coordinator.Trigger.Polling polling, JobRunrSchedules jobRunrSchedules) {

        this.jobRunrSchedules = jobRunrSchedules;
        this.pollingTriggerCheckPeriod = Duration.ofMinutes(polling.getCheckPeriod());
    }

    @Override
    public void cancelDynamicWebhookTriggerRefresh(String workflowExecutionId) {
        jobRunrSchedules.cancelOnce(getDynamicWebhookTriggerRefreshKey(workflowExecutionId));
    }

    @Override
    public void cancelOneTimeTask(long jobId) {
        jobRunrSchedules.cancelOnce(getOneTimeTaskKey(TenantContext.getCurrentTenantId(), jobId));
    }

    @Override
    public void cancelScheduleTrigger(String workflowExecutionId) {
        jobRunrSchedules.cancelRecurring(JobRunrIds.recurringJobBaseId(SCHEDULE_TRIGGER, workflowExecutionId));
    }

    @Override
    public void cancelPollingTrigger(String workflowExecutionId) {
        jobRunrSchedules.cancelRecurring(JobRunrIds.recurringJobBaseId(POLLING_TRIGGER, workflowExecutionId));
    }

    @Override
    public void scheduleDynamicWebhookTriggerRefresh(
        Instant webhookExpirationDate, String componentName, int componentVersion,
        WorkflowExecutionId workflowExecutionId, Long connectionId) {

        String workflowExecutionIdString = workflowExecutionId.toString();

        jobRunrSchedules.scheduleOnce(
            getDynamicWebhookTriggerRefreshKey(workflowExecutionIdString), webhookExpirationDate,
            "Dynamic webhook refresh of trigger %s in workflow %s".formatted(
                workflowExecutionId.getTriggerName(), workflowExecutionId.getWorkflowUuid()),
            getLabels(DYNAMIC_WEBHOOK_TRIGGER_REFRESH, workflowExecutionId.getTenantId()),
            jobs -> jobs.executeDynamicWebhookTriggerRefresh(workflowExecutionIdString, connectionId));
    }

    @Override
    public void scheduleScheduleTrigger(
        String pattern, String zoneId, Map<String, Object> output, WorkflowExecutionId workflowExecutionId) {

        String cron = QuartzCronExpressionConverter.convert(pattern);
        String outputJson = JsonUtils.write(output);
        String workflowExecutionIdString = workflowExecutionId.toString();

        jobRunrSchedules.scheduleRecurring(
            JobRunrIds.recurringJobBaseId(SCHEDULE_TRIGGER, workflowExecutionIdString),
            RecurringJobBuilder.aRecurringJob()
                .withName(
                    "Schedule trigger %s in workflow %s".formatted(
                        workflowExecutionId.getTriggerName(), workflowExecutionId.getWorkflowUuid()))
                .withLabels(getLabels(SCHEDULE_TRIGGER, workflowExecutionId.getTenantId()))
                .withCron(cron)
                .withZoneId(QuartzCronExpressionConverter.getZoneId(pattern, ZoneId.of(zoneId)))
                .withJobLambda(
                    (JobRunrSchedulerJobs jobs) -> jobs.executeScheduleTrigger(workflowExecutionIdString, outputJson)));
    }

    /**
     * Polls every {@code bytechef.coordinator.trigger.polling.check-period} minutes. Like the Quartz provider, which
     * starts the repeating trigger now, the first poll runs right away.
     */
    @Override
    public void schedulePollingTrigger(WorkflowExecutionId workflowExecutionId) {
        String workflowExecutionIdString = workflowExecutionId.toString();
        String jobName = "Polling trigger %s in workflow %s".formatted(
            workflowExecutionId.getTriggerName(), workflowExecutionId.getWorkflowUuid());
        List<String> labels = getLabels(POLLING_TRIGGER, workflowExecutionId.getTenantId());

        jobRunrSchedules.scheduleRecurring(
            JobRunrIds.recurringJobBaseId(POLLING_TRIGGER, workflowExecutionIdString),
            RecurringJobBuilder.aRecurringJob()
                .withName(jobName)
                .withLabels(labels)
                .withInterval(pollingTriggerCheckPeriod)
                .withJobLambda((JobRunrSchedulerJobs jobs) -> jobs.executePollingTrigger(workflowExecutionIdString)));

        jobRunrSchedules.enqueue(jobName, labels, jobs -> jobs.executePollingTrigger(workflowExecutionIdString));
    }

    /**
     * Unlike the Quartz provider, the key contains the tenant, so one-time tasks of jobs with the same id in different
     * tenants do not replace each other.
     */
    @Override
    public void scheduleOneTimeTask(Instant executeAt, Map<String, ?> output, long jobId) {
        String tenantId = TenantContext.getCurrentTenantId();
        String continueParameters = output == null || output.isEmpty() ? "" : JsonUtils.write(output);

        jobRunrSchedules.scheduleOnce(
            getOneTimeTaskKey(tenantId, jobId), executeAt, "Resume job %s".formatted(jobId),
            getLabels(ONE_TIME_TASK, tenantId),
            jobs -> jobs.executeOneTimeTask(tenantId, jobId, continueParameters));
    }

    private static String getDynamicWebhookTriggerRefreshKey(String workflowExecutionId) {
        return DYNAMIC_WEBHOOK_TRIGGER_REFRESH + ":" + workflowExecutionId;
    }

    private static List<String> getLabels(String kind, String tenantId) {
        return List.of(kind, "tenant:" + tenantId);
    }

    private static String getOneTimeTaskKey(String tenantId, long jobId) {
        return ONE_TIME_TASK + ":" + tenantId + ":" + jobId;
    }
}
