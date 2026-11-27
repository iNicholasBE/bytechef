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

import com.bytechef.atlas.configuration.domain.Workflow;
import com.bytechef.atlas.configuration.service.WorkflowService;
import com.bytechef.atlas.coordinator.event.ResumeJobEvent;
import com.bytechef.commons.util.JsonUtils;
import com.bytechef.commons.util.MapUtils;
import com.bytechef.commons.util.OptionalUtils;
import com.bytechef.component.definition.TriggerDefinition.WebhookEnableOutput;
import com.bytechef.platform.billing.facade.BillingUsageFacade;
import com.bytechef.platform.component.facade.TriggerDefinitionFacade;
import com.bytechef.platform.configuration.domain.WorkflowTrigger;
import com.bytechef.platform.connection.facade.ConnectionFacade;
import com.bytechef.platform.definition.WorkflowNodeType;
import com.bytechef.platform.scheduler.ConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.TriggerScheduler;
import com.bytechef.platform.security.util.SecurityUtils;
import com.bytechef.platform.workflow.WorkflowExecutionId;
import com.bytechef.platform.workflow.coordinator.event.TriggerListenerEvent;
import com.bytechef.platform.workflow.coordinator.event.TriggerPollEvent;
import com.bytechef.platform.workflow.execution.JobResumeId;
import com.bytechef.platform.workflow.execution.accessor.JobPrincipalAccessor;
import com.bytechef.platform.workflow.execution.accessor.JobPrincipalAccessorRegistry;
import com.bytechef.platform.workflow.execution.facade.JobResumeFacade;
import com.bytechef.platform.workflow.execution.facade.JobResumeFacade.JobResumeOutcome;
import com.bytechef.platform.workflow.execution.facade.TriggerStateFacade;
import com.bytechef.tenant.TenantContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;

/**
 * The bodies of the JobRunr jobs, one public method per job kind, repeating the logic of the Quartz jobs in
 * {@code platform-scheduler-impl}. JobRunr stores a call to one of these methods with its arguments, so every argument
 * is a string or a number and maps travel as JSON, as they do in the Quartz job data maps. Each method runs under the
 * system principal, as {@code SystemSecurityContextJob} does for Quartz.
 *
 * <p>
 * Each method first waits until the moment its run was scheduled for ({@link ScheduledFireTimeFilter}), because JobRunr
 * OSS can start a scheduled job up to one poll interval early.
 *
 * <p>
 * The methods are part of the stored jobs: renaming one or changing its parameters fails the jobs that are scheduled
 * with the old signature.
 *
 * @author Nicholas D'hondt
 */
public class JobRunrSchedulerJobs {

    private static final AtomicBoolean UNCHECKED_RESUME_WARNED = new AtomicBoolean();

    private static final Logger log = LoggerFactory.getLogger(JobRunrSchedulerJobs.class);

    private final ObjectProvider<BillingUsageFacade> billingUsageFacadeProvider;
    private final ObjectProvider<ConnectionFacade> connectionFacadeProvider;
    private final ConnectionRefreshScheduler connectionRefreshScheduler;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectProvider<JobPrincipalAccessorRegistry> jobPrincipalAccessorRegistryProvider;
    private final ObjectProvider<JobResumeFacade> jobResumeFacadeProvider;
    private final ObjectProvider<TriggerDefinitionFacade> triggerDefinitionFacadeProvider;
    private final TriggerScheduler triggerScheduler;
    private final ObjectProvider<TriggerStateFacade> triggerStateFacadeProvider;
    private final ObjectProvider<WorkflowService> workflowServiceProvider;

    @SuppressFBWarnings("EI")
    @SuppressWarnings("PMD.ExcessiveParameterList")
    public JobRunrSchedulerJobs(
        ObjectProvider<BillingUsageFacade> billingUsageFacadeProvider,
        ObjectProvider<ConnectionFacade> connectionFacadeProvider,
        ConnectionRefreshScheduler connectionRefreshScheduler, ApplicationEventPublisher eventPublisher,
        ObjectProvider<JobPrincipalAccessorRegistry> jobPrincipalAccessorRegistryProvider,
        ObjectProvider<JobResumeFacade> jobResumeFacadeProvider,
        ObjectProvider<TriggerDefinitionFacade> triggerDefinitionFacadeProvider, TriggerScheduler triggerScheduler,
        ObjectProvider<TriggerStateFacade> triggerStateFacadeProvider,
        ObjectProvider<WorkflowService> workflowServiceProvider) {

        this.billingUsageFacadeProvider = billingUsageFacadeProvider;
        this.connectionFacadeProvider = connectionFacadeProvider;
        this.connectionRefreshScheduler = connectionRefreshScheduler;
        this.eventPublisher = eventPublisher;
        this.jobPrincipalAccessorRegistryProvider = jobPrincipalAccessorRegistryProvider;
        this.jobResumeFacadeProvider = jobResumeFacadeProvider;
        this.triggerDefinitionFacadeProvider = triggerDefinitionFacadeProvider;
        this.triggerScheduler = triggerScheduler;
        this.triggerStateFacadeProvider = triggerStateFacadeProvider;
        this.workflowServiceProvider = workflowServiceProvider;
    }

    /**
     * Refreshes the OAuth2 token of the connection and schedules the next refresh, as
     * {@code ConnectionOAuth2TokenRefreshJob} does.
     */
    public void executeConnectionRefresh(long connectionId, String tenantId) throws InterruptedException {
        ScheduledFireTimeFilter.awaitScheduledTime();

        runAsSystem(() -> {
            ConnectionFacade connectionFacade = connectionFacadeProvider.getObject();

            Integer expiresIn = TenantContext.callWithTenantId(
                tenantId, () -> connectionFacade.executeConnectionRefresh(connectionId));

            if (expiresIn == null) {
                log.warn("Connection {} returned no token lifetime; no next refresh is scheduled", connectionId);

                return;
            }

            Instant now = Instant.now();

            connectionRefreshScheduler.scheduleConnectionRefresh(connectionId, now.plusSeconds(expiresIn), tenantId);
        });
    }

    /**
     * Refreshes the dynamic webhook and schedules the next refresh at its new expiration date, as
     * {@code DynamicWebhookTriggerRefreshJob} does.
     */
    public void executeDynamicWebhookTriggerRefresh(String workflowExecutionId, @Nullable Long connectionId)
        throws InterruptedException {
        ScheduledFireTimeFilter.awaitScheduledTime();

        runAsSystem(() -> {
            WorkflowExecutionId parsedWorkflowExecutionId = WorkflowExecutionId.parse(workflowExecutionId);

            WorkflowNodeType workflowNodeType = getWorkflowNodeType(parsedWorkflowExecutionId);
            TriggerStateFacade triggerStateFacade = triggerStateFacadeProvider.getObject();
            TriggerDefinitionFacade triggerDefinitionFacade = triggerDefinitionFacadeProvider.getObject();

            WebhookEnableOutput output = OptionalUtils.get(triggerStateFacade.fetchValue(parsedWorkflowExecutionId));

            output = triggerDefinitionFacade.executeDynamicWebhookRefresh(
                workflowNodeType.name(), workflowNodeType.version(), workflowNodeType.operation(),
                output.parameters(), connectionId);

            if (output == null) {
                return;
            }

            triggerStateFacade.save(parsedWorkflowExecutionId, output);

            Instant webhookExpirationDate = output.webhookExpirationDate();

            if (webhookExpirationDate != null) {
                triggerScheduler.scheduleDynamicWebhookTriggerRefresh(
                    webhookExpirationDate, workflowNodeType.name(), workflowNodeType.version(),
                    parsedWorkflowExecutionId, connectionId);
            }
        });
    }

    /**
     * Resumes a job when its suspend deadline passes, as {@code OneTimeSchedulerJob} does. While the job has not
     * reached the suspend yet, or the resume fails, the method throws and JobRunr retries it with the delays and the
     * number of attempts of the Quartz job ({@link OneTimeTaskRetryFilter}). A task without a resume id, or one whose
     * resume id cannot be checked because no local {@link JobResumeFacade} is available, resumes the job
     * unconditionally.
     *
     * @param continueParameters the task output as JSON, or an empty string when there is none
     */
    public void executeOneTimeTask(String tenantId, long jobId, String continueParameters) throws InterruptedException {
        ScheduledFireTimeFilter.awaitScheduledTime();

        runAsSystem(() -> TenantContext.runWithTenantId(tenantId, () -> {
            String jobResumeId = getJobResumeId(continueParameters, jobId);
            JobResumeFacade jobResumeFacade = jobResumeFacadeProvider.getIfAvailable();

            if (jobResumeId != null && jobResumeFacade != null) {
                try {
                    JobResumeOutcome jobResumeOutcome = jobResumeFacade.resumeExpiredJob(jobResumeId);

                    if (jobResumeOutcome == JobResumeOutcome.NOT_YET_SUSPENDED) {
                        throw new IllegalStateException(
                            "Job " + jobId + " has not reached its suspend yet; its expiry is retried");
                    }

                    return;
                } catch (UnsupportedOperationException unsupportedOperationException) {
                    if (UNCHECKED_RESUME_WARNED.compareAndSet(false, true)) {
                        log.warn(
                            "Expired suspends cannot be checked by this node's JobResumeFacade; jobs are resumed " +
                                "without checking that they still wait on the expired suspend");
                    }
                }
            }

            eventPublisher.publishEvent(new ResumeJobEvent(jobId));
        }));
    }

    /**
     * Publishes a {@link TriggerPollEvent}, as {@code PollingTriggerJob} does.
     */
    public void executePollingTrigger(String workflowExecutionId) throws InterruptedException {
        ScheduledFireTimeFilter.awaitScheduledTime();

        runAsSystem(
            () -> eventPublisher.publishEvent(new TriggerPollEvent(WorkflowExecutionId.parse(workflowExecutionId))));
    }

    /**
     * Publishes a {@link TriggerListenerEvent} with the moment the run was scheduled for, as {@code ScheduleTriggerJob}
     * does with the Quartz fire time.
     */
    public void executeScheduleTrigger(String workflowExecutionId, String outputJson) throws InterruptedException {
        ScheduledFireTimeFilter.awaitScheduledTime();

        Instant fireTime = ScheduledFireTimeFilter.getFireTime();

        runAsSystem(() -> {
            Map<String, Object> output = JsonUtils.readMap(outputJson, Object.class);
            Date fireDate = Date.from(fireTime);

            eventPublisher.publishEvent(
                new TriggerListenerEvent(
                    new TriggerListenerEvent.ListenerParameters(
                        WorkflowExecutionId.parse(workflowExecutionId), fireTime,
                        MapUtils.concat(
                            Map.of("fireTime", fireDate, "dateTime", getFireLocalDateTime(output, fireTime)),
                            output))));
        });
    }

    /**
     * Reports usage to Stripe, as {@code StripeUsageReportingJob} does.
     */
    public void executeStripeUsageReport() throws InterruptedException {
        ScheduledFireTimeFilter.awaitScheduledTime();

        runAsSystem(() -> {
            log.info("Stripe usage reporting triggered");

            BillingUsageFacade billingUsageFacade = billingUsageFacadeProvider.getObject();

            billingUsageFacade.reportUsage(Instant.now());
        });
    }

    /**
     * The fire time in the time zone of the trigger, see {@code ScheduleTriggerJob#getFireLocalDateTime}.
     */
    private static LocalDateTime getFireLocalDateTime(Map<String, ?> output, Instant fireTime) {
        ZoneId zoneId = ZoneId.systemDefault();

        if (output.get("timezone") instanceof String timezone) {
            zoneId = ZoneId.of(timezone);
        }

        if (Objects.equals(zoneId, ZoneId.systemDefault())) {
            log.info("Default ZoneId is used. Workflow definition parameters miss zone id - check/update db values");
        }

        return fireTime.atZone(zoneId)
            .toLocalDateTime();
    }

    private static @Nullable String getJobResumeId(String continueParameters, long jobId) {
        if (continueParameters.isEmpty()) {
            return null;
        }

        try {
            Map<String, Object> continueParameterMap = JsonUtils.readMap(continueParameters, Object.class);

            return continueParameterMap.get(JobResumeId.TIMEOUT_TASK_PARAMETER) instanceof String jobResumeId
                ? jobResumeId : null;
        } catch (RuntimeException exception) {
            log.warn("Unable to read the resume id of job {}'s expired suspend", jobId, exception);

            return null;
        }
    }

    private WorkflowNodeType getWorkflowNodeType(WorkflowExecutionId workflowExecutionId) {
        JobPrincipalAccessorRegistry jobPrincipalAccessorRegistry = jobPrincipalAccessorRegistryProvider.getObject();

        JobPrincipalAccessor jobPrincipalAccessor = jobPrincipalAccessorRegistry.getJobPrincipalAccessor(
            workflowExecutionId.getType());

        String workflowId = jobPrincipalAccessor.getWorkflowId(
            workflowExecutionId.getJobPrincipalId(), workflowExecutionId.getWorkflowUuid());

        WorkflowService workflowService = workflowServiceProvider.getObject();

        Workflow workflow = workflowService.getWorkflow(workflowId);

        WorkflowTrigger workflowTrigger = WorkflowTrigger.of(workflowExecutionId.getTriggerName(), workflow);

        return WorkflowNodeType.ofType(workflowTrigger.getType());
    }

    private static void runAsSystem(Runnable runnable) {
        SecurityUtils.runAsSystem(() -> {
            runnable.run();

            return null;
        });
    }
}
