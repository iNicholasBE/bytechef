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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytechef.atlas.coordinator.event.ResumeJobEvent;
import com.bytechef.config.ApplicationProperties;
import com.bytechef.platform.connection.facade.ConnectionFacade;
import com.bytechef.platform.constant.PlatformType;
import com.bytechef.platform.scheduler.ConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.TriggerScheduler;
import com.bytechef.platform.scheduler.jobrunr.config.JobRunrSchedulerIntTestConfiguration;
import com.bytechef.platform.scheduler.jobrunr.job.OneTimeTaskRetryFilter;
import com.bytechef.platform.scheduler.jobrunr.job.ScheduledFireTimeFilter;
import com.bytechef.platform.scheduler.jobrunr.util.JobRunrIds;
import com.bytechef.platform.workflow.WorkflowExecutionId;
import com.bytechef.platform.workflow.coordinator.event.TriggerListenerEvent;
import com.bytechef.platform.workflow.coordinator.event.TriggerPollEvent;
import com.bytechef.platform.workflow.execution.JobResumeId;
import com.bytechef.platform.workflow.execution.facade.JobResumeFacade;
import com.bytechef.platform.workflow.execution.facade.JobResumeFacade.JobResumeOutcome;
import com.bytechef.tenant.TenantContext;
import com.bytechef.test.config.testcontainers.PostgreSQLContainerConfiguration;
import com.bytechef.test.extension.ObjectMapperSetupExtension;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.server.BackgroundJobServerConfiguration;
import org.jobrunr.storage.JobNotFoundException;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.utils.mapper.JsonMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;

/**
 * Runs every {@link TriggerScheduler} and {@link ConnectionRefreshScheduler} operation on JobRunr against PostgreSQL,
 * with a real background job server (poll interval 5 seconds, the JobRunr minimum), and checks what fires and when.
 *
 * @author Nicholas D'hondt
 */
@ExtendWith(ObjectMapperSetupExtension.class)
@SpringBootTest(
    classes = JobRunrSchedulerIntTestConfiguration.class,
    properties = {
        "bytechef.scheduler.provider=jobrunr", "bytechef.coordinator.enabled=false"
    })
@Import({
    PostgreSQLContainerConfiguration.class,
    JobRunrTriggerSchedulerIntTest.JobRunrTriggerSchedulerIntTestConfiguration.class
})
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class JobRunrTriggerSchedulerIntTest {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(5);

    @Autowired
    private ConnectionFacade connectionFacade;

    @Autowired
    private ConnectionRefreshScheduler connectionRefreshScheduler;

    @Autowired
    private EventCollector eventCollector;

    @Autowired
    private JobResumeFacade jobResumeFacade;

    @Autowired
    private StorageProvider storageProvider;

    @Autowired
    private TriggerScheduler triggerScheduler;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private JsonMapper jobRunrJsonMapper;

    @AfterEach
    void afterEach() {
        for (RecurringJob recurringJob : storageProvider.getRecurringJobs()) {
            storageProvider.deleteRecurringJob(recurringJob.getId());
        }

        eventCollector.clear();

        reset(connectionFacade, jobResumeFacade);
    }

    @Test
    void testScheduleScheduleTriggerFiresOnTheCronWithTheScheduledFireTime() {
        WorkflowExecutionId workflowExecutionId = workflowExecutionId("every-10-seconds");

        triggerScheduler.scheduleScheduleTrigger(
            "0/10 * * ? * *", "Europe/Zagreb", Map.of("timezone", "Europe/Zagreb", "key", "value"),
            workflowExecutionId);

        await().atMost(Duration.ofSeconds(40))
            .until(() -> eventCollector.listenerEvents(workflowExecutionId)
                .size() >= 2);

        for (ReceivedEvent<TriggerListenerEvent> receivedEvent : eventCollector.listenerEvents(workflowExecutionId)) {
            TriggerListenerEvent.ListenerParameters listenerParameters = receivedEvent.event()
                .getListenerParameters();
            Instant fireTime = listenerParameters.executionDate();

            assertThat(receivedEvent.receivedAt()).isAfterOrEqualTo(fireTime);
            assertThat(fireTime.getEpochSecond() % 10).isZero();
            assertThat(fireTime.getNano()).isZero();
            assertThat(listenerParameters.output()).asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .containsEntry("key", "value")
                .containsKeys("fireTime", "dateTime");

            System.out.printf(
                "FIRE_TIME_DELTA scheduled=%s received=%s deltaMillis=%d%n", fireTime, receivedEvent.receivedAt(),
                Duration.between(fireTime, receivedEvent.receivedAt())
                    .toMillis());
        }

        triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());

        assertNoMoreEvents(() -> eventCollector.listenerEvents(workflowExecutionId)
            .size(), Duration.ofSeconds(20));
    }

    /**
     * A cancel removes the recurring job at once. The run JobRunr already created ahead of time still starts, but does
     * nothing because its generation is no longer current, so no trigger fires after the cancel.
     */
    @Test
    void testCancelScheduleTriggerRemovesTheRecurringJobAndNothingFiresAfterwards() {
        WorkflowExecutionId workflowExecutionId = workflowExecutionId("cancel-pending");
        String baseId = JobRunrIds.recurringJobBaseId("schedule-trigger", workflowExecutionId.toString());

        triggerScheduler.scheduleScheduleTrigger("0/10 * * ? * *", "UTC", Map.of(), workflowExecutionId);

        await().atMost(Duration.ofSeconds(20))
            .until(() -> getPendingRuns(baseId).size() == 1);

        triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());

        assertThat(getRecurringJobIds(baseId)).isEmpty();
        assertNoMoreEvents(() -> eventCollector.listenerEvents(workflowExecutionId)
            .size(), Duration.ofSeconds(20));
    }

    @Test
    void testRescheduleScheduleTriggerTakesEffectImmediatelyAndKeepsOneRecurringJob() {
        WorkflowExecutionId workflowExecutionId = workflowExecutionId("reschedule");
        String baseId = JobRunrIds.recurringJobBaseId("schedule-trigger", workflowExecutionId.toString());

        triggerScheduler.scheduleScheduleTrigger("0 0 12 ? * *", "UTC", Map.of(), workflowExecutionId);
        triggerScheduler.scheduleScheduleTrigger("0/10 * * ? * *", "UTC", Map.of(), workflowExecutionId);

        assertThat(getRecurringJobIds(baseId)).hasSize(1);

        await().atMost(Duration.ofSeconds(30))
            .until(() -> !eventCollector.listenerEvents(workflowExecutionId)
                .isEmpty());

        triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());
        triggerScheduler.scheduleScheduleTrigger("0/10 * * ? * *", "UTC", Map.of(), workflowExecutionId);

        int eventCountAfterReEnable = eventCollector.listenerEvents(workflowExecutionId)
            .size();

        await().atMost(Duration.ofSeconds(30))
            .until(() -> eventCollector.listenerEvents(workflowExecutionId)
                .size() > eventCountAfterReEnable);

        // no burst of missed runs after cancel and schedule under the same key
        assertThat(eventCollector.listenerEvents(workflowExecutionId)).hasSizeLessThanOrEqualTo(
            eventCountAfterReEnable + 2);

        triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());
    }

    @Test
    void testScheduleScheduleTriggerRefusesCronFeaturesJobRunrOssDoesNotHave() {
        WorkflowExecutionId workflowExecutionId = workflowExecutionId("last-day");

        org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> triggerScheduler.scheduleScheduleTrigger("0 0 0 L * ?", "UTC", Map.of(), workflowExecutionId))
            .isInstanceOf(IllegalArgumentException.class);

        assertThat(storageProvider.getRecurringJobs()).isEmpty();
    }

    @Test
    void testSchedulePollingTriggerPollsRightAwayAndCancels() {
        WorkflowExecutionId workflowExecutionId = workflowExecutionId("polling");
        String baseId = JobRunrIds.recurringJobBaseId("polling-trigger", workflowExecutionId.toString());

        triggerScheduler.schedulePollingTrigger(workflowExecutionId);

        await().atMost(Duration.ofSeconds(15))
            .until(() -> eventCollector.pollEvents(workflowExecutionId) == 1);

        List<String> recurringJobIds = getRecurringJobIds(baseId);

        assertThat(recurringJobIds).hasSize(1);
        assertThat(getRecurringJob(recurringJobIds.getFirst()).getScheduleExpression()).isEqualTo("PT5M");

        triggerScheduler.cancelPollingTrigger(workflowExecutionId.toString());

        assertThat(getRecurringJobIds(baseId)).isEmpty();
    }

    @Test
    void testScheduleOneTimeTaskRunsAtTheInstant() {
        Instant executeAt = Instant.now()
            .plusSeconds(8);

        triggerScheduler.scheduleOneTimeTask(executeAt, Map.of(), 1001L);

        await().atMost(Duration.ofSeconds(30))
            .until(() -> eventCollector.resumeEvents(1001L) == 1);

        assertThat(eventCollector.resumeReceivedAt(1001L)).isAfterOrEqualTo(executeAt);

        System.out.printf(
            "ONE_TIME_DELTA executeAt=%s received=%s deltaMillis=%d%n", executeAt,
            eventCollector.resumeReceivedAt(1001L), Duration.between(executeAt, eventCollector.resumeReceivedAt(1001L))
                .toMillis());
    }

    /**
     * Trap: JobRunr OSS silently skips creating a job under an id that still exists. Rescheduling a key must still
     * replace the pending task, run it once at the new instant, and cancelling must stop it.
     */
    @Test
    void testRescheduleOneTimeTaskRunsOnceAtTheNewInstantAndCancelStopsIt() {
        Instant later = Instant.now()
            .plusSeconds(60);
        Instant sooner = Instant.now()
            .plusSeconds(6);

        triggerScheduler.scheduleOneTimeTask(later, Map.of(), 1002L);
        triggerScheduler.scheduleOneTimeTask(sooner, Map.of(), 1002L);

        await().atMost(Duration.ofSeconds(30))
            .until(() -> eventCollector.resumeEvents(1002L) == 1);

        assertThat(eventCollector.resumeReceivedAt(1002L)).isBefore(later)
            .isAfterOrEqualTo(sooner);

        triggerScheduler.scheduleOneTimeTask(Instant.now()
            .plusSeconds(6), Map.of(), 1003L);
        triggerScheduler.cancelOneTimeTask(1003L);

        assertNoMoreEvents(() -> eventCollector.resumeEvents(1002L) + eventCollector.resumeEvents(1003L),
            Duration.ofSeconds(20));
        assertThat(eventCollector.resumeEvents(1002L)).isEqualTo(1);
        assertThat(eventCollector.resumeEvents(1003L)).isZero();
    }

    @Test
    void testCancelOneTimeTaskWithoutAScheduledTaskDoesNothing() {
        triggerScheduler.cancelOneTimeTask(1004L);
    }

    @Test
    void testOneTimeTasksOfTheSameJobIdInTwoTenantsDoNotReplaceEachOther() {
        Instant executeAt = Instant.now()
            .plusSeconds(6);

        TenantContext.runWithTenantId("tenant_a",
            () -> triggerScheduler.scheduleOneTimeTask(executeAt, Map.of(), 1005L));
        TenantContext.runWithTenantId("tenant_b",
            () -> triggerScheduler.scheduleOneTimeTask(executeAt, Map.of(), 1005L));

        await().atMost(Duration.ofSeconds(30))
            .until(() -> eventCollector.resumeEvents(1005L) == 2);
    }

    /**
     * The one-time task retries with the delays of the Quartz job while the job has not reached its suspend, and a
     * cancel also stops a task that is waiting for its next attempt.
     */
    @Test
    void testOneTimeTaskRetriesUntilTheJobIsSuspendedAndCancelStopsTheRetries() {
        when(jobResumeFacade.resumeExpiredJob("resume-1")).thenReturn(
            JobResumeOutcome.NOT_YET_SUSPENDED, JobResumeOutcome.OK);
        when(jobResumeFacade.resumeExpiredJob("resume-2")).thenReturn(JobResumeOutcome.NOT_YET_SUSPENDED);

        triggerScheduler.scheduleOneTimeTask(
            Instant.now(), Map.of(JobResumeId.TIMEOUT_TASK_PARAMETER, "resume-1"), 1006L);

        verify(jobResumeFacade, timeout(30_000).times(2)).resumeExpiredJob("resume-1");

        await().atMost(Duration.ofSeconds(10))
            .until(() -> getOneTimeTaskJob(1006L).getState() == StateName.SUCCEEDED);

        Job job = getOneTimeTaskJob(1006L);

        assertThat(job.getJobStatesOfType(FailedState.class)
            .count()).isEqualTo(1);

        triggerScheduler.scheduleOneTimeTask(
            Instant.now(), Map.of(JobResumeId.TIMEOUT_TASK_PARAMETER, "resume-2"), 1007L);

        verify(jobResumeFacade, timeout(30_000).times(1)).resumeExpiredJob("resume-2");

        await().atMost(Duration.ofSeconds(20))
            .pollInterval(Duration.ofMillis(50))
            .until(() -> {
                Job retryingJob = getOneTimeTaskJob(1007L);
                StateName stateName = retryingJob.getState();

                return (stateName == StateName.SCHEDULED || stateName == StateName.ENQUEUED) &&
                    retryingJob.getJobStatesOfType(FailedState.class)
                        .count() >= 1;
            });

        triggerScheduler.cancelOneTimeTask(1007L);

        assertThat(findOneTimeTaskJob(1007L)).isNull();
        assertNoMoreEvents(() -> mockingCallCount("resume-2"), Duration.ofSeconds(15));
        assertThat(eventCollector.resumeEvents(1007L)).isZero();
    }

    @Test
    void testScheduleAndCancelDynamicWebhookTriggerRefresh() {
        WorkflowExecutionId withConnection = workflowExecutionId("webhook-refresh");
        WorkflowExecutionId withoutConnection = workflowExecutionId("webhook-refresh-without-connection");
        Instant webhookExpirationDate = Instant.now()
            .plus(Duration.ofHours(1));

        triggerScheduler.scheduleDynamicWebhookTriggerRefresh(
            webhookExpirationDate, "testComponent", 1, withConnection, 456L);
        triggerScheduler.scheduleDynamicWebhookTriggerRefresh(
            webhookExpirationDate, "testComponent", 1, withoutConnection, null);

        Job job = storageProvider.getJobById(
            JobRunrIds.jobId("dynamic-webhook-trigger-refresh:" + withConnection, 0));

        assertThat(job.getState()).isEqualTo(StateName.SCHEDULED);
        assertThat(job.getJobDetails()
            .getCacheable()).isTrue();
        assertThat(job.<ScheduledState>getJobState()
            .getScheduledAt()).isEqualTo(webhookExpirationDate);
        assertThat(job.getJobDetails()
            .getJobParameterValues()).containsExactly(withConnection.toString(), 456L);
        assertThat(storageProvider.getJobById(
            JobRunrIds.jobId("dynamic-webhook-trigger-refresh:" + withoutConnection, 0))
            .getJobDetails()
            .getJobParameterValues()).containsExactly(withoutConnection.toString(), null);

        triggerScheduler.cancelDynamicWebhookTriggerRefresh(withConnection.toString());
        triggerScheduler.cancelDynamicWebhookTriggerRefresh(withoutConnection.toString());

        assertThat(storageProvider.getJobList(StateName.SCHEDULED, org.jobrunr.storage.Paging.AmountBasedList
            .ascOnUpdatedAt(1000))).noneMatch(scheduledJob -> scheduledJob.getJobName()
                .startsWith("Dynamic webhook refresh"));
    }

    /**
     * The token refresh job reschedules its own key while it runs. That lands in the second slot, so the next refresh
     * exists although the running job still occupies the first.
     */
    @Test
    void testConnectionRefreshRunsAndSchedulesTheNextRefresh() {
        when(connectionFacade.executeConnectionRefresh(77L)).thenReturn(3600);

        connectionRefreshScheduler.scheduleConnectionRefresh(77L, Instant.now()
            .plus(Duration.ofMinutes(5))
            .plusSeconds(4), "public");

        verify(connectionFacade, timeout(30_000)).executeConnectionRefresh(77L);

        await().atMost(Duration.ofSeconds(10))
            .until(() -> getScheduledConnectionRefreshes(77L).size() == 1);

        Job nextRefresh = getScheduledConnectionRefreshes(77L).getFirst();
        Instant nextRefreshAt = nextRefresh.<ScheduledState>getJobState()
            .getScheduledAt();

        assertThat(nextRefreshAt).isBetween(
            Instant.now()
                .plus(Duration.ofMinutes(54)),
            Instant.now()
                .plus(Duration.ofMinutes(56)));

        connectionRefreshScheduler.cancelConnectionRefresh(77L, "public");

        assertThat(getScheduledConnectionRefreshes(77L)).isEmpty();
    }

    /**
     * Trap: what does JobRunr OSS do with a recurring job whose runs keep failing? Records the runs over 70 seconds of
     * a trigger that fires every 10 seconds and always fails.
     */
    @Test
    void testFailingScheduleTriggerIsRetriedAndHoldsBackNewRuns() {
        WorkflowExecutionId workflowExecutionId = workflowExecutionId("always-failing");
        String baseId = JobRunrIds.recurringJobBaseId("schedule-trigger", workflowExecutionId.toString());

        eventCollector.failFor(workflowExecutionId);

        triggerScheduler.scheduleScheduleTrigger("0/10 * * ? * *", "UTC", Map.of(), workflowExecutionId);

        await().pollDelay(Duration.ofSeconds(70))
            .atMost(Duration.ofSeconds(71))
            .until(() -> true);

        String recurringJobId = getRecurringJobIds(baseId).getFirst();
        List<Job> runs = new ArrayList<>();

        for (StateName stateName : StateName.values()) {
            storageProvider.getJobList(stateName, org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt(1000))
                .stream()
                .filter(run -> recurringJobId.equals(run.getRecurringJobId()
                    .orElse(null)))
                .forEach(runs::add);
        }

        int attempts = eventCollector.listenerEvents(workflowExecutionId)
            .size();

        System.out.printf(
            "FAILING_RECURRING runs=%d attempts=%d states=%s%n", runs.size(), attempts,
            runs.stream()
                .map(run -> run.getState() + "/failures=" + run.getJobStatesOfType(FailedState.class)
                    .count())
                .toList());

        // 7 ticks in 70 seconds; while a run waits for its retry no new run is created
        assertThat(runs).hasSizeLessThan(4);

        triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());
    }

    /**
     * Two background job servers on the same database (two coordinators): each tick of a schedule trigger fires once.
     */
    @Test
    void testTwoBackgroundJobServersFireEachTickOnce() {
        BackgroundJobServer secondBackgroundJobServer = createBackgroundJobServer(
            storageProvider, jobRunrJsonMapper, applicationContext);

        try {
            WorkflowExecutionId workflowExecutionId = workflowExecutionId("two-servers");

            triggerScheduler.scheduleScheduleTrigger("0/10 * * ? * *", "UTC", Map.of(), workflowExecutionId);

            await().atMost(Duration.ofSeconds(60))
                .until(() -> eventCollector.listenerEvents(workflowExecutionId)
                    .size() >= 4);

            List<Instant> fireTimes = eventCollector.listenerEvents(workflowExecutionId)
                .stream()
                .map(receivedEvent -> receivedEvent.event()
                    .getListenerParameters()
                    .executionDate())
                .toList();

            assertThat(fireTimes).doesNotHaveDuplicates();

            triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());
        } finally {
            secondBackgroundJobServer.stop();
        }
    }

    /**
     * Trap: the JobRunr documentation states that OSS supports up to 100 recurring jobs. The sources of 9.0.5 have no
     * such limit in code; this test schedules 150 triggers and expects every one to fire.
     */
    @Test
    void testMoreThanOneHundredScheduleTriggersAllFire() {
        List<WorkflowExecutionId> workflowExecutionIds = IntStream.range(0, 150)
            .mapToObj(index -> workflowExecutionId("many-" + index))
            .toList();

        for (WorkflowExecutionId workflowExecutionId : workflowExecutionIds) {
            triggerScheduler.scheduleScheduleTrigger("0/10 * * ? * *", "UTC", Map.of(), workflowExecutionId);
        }

        assertThat(storageProvider.getRecurringJobs()).hasSize(150);

        await().atMost(Duration.ofSeconds(60))
            .until(() -> workflowExecutionIds.stream()
                .allMatch(workflowExecutionId -> !eventCollector.listenerEvents(workflowExecutionId)
                    .isEmpty()));

        for (WorkflowExecutionId workflowExecutionId : workflowExecutionIds) {
            triggerScheduler.cancelScheduleTrigger(workflowExecutionId.toString());
        }

        assertThat(storageProvider.getRecurringJobs()).isEmpty();
    }

    private void assertNoMoreEvents(java.util.function.IntSupplier eventCount, Duration duration) {
        int before = eventCount.getAsInt();

        await().pollDelay(duration)
            .atMost(duration.plusSeconds(1))
            .until(() -> true);

        assertThat(eventCount.getAsInt()).isEqualTo(before);
    }

    private int mockingCallCount(String resumeId) {
        return (int) org.mockito.Mockito.mockingDetails(jobResumeFacade)
            .getInvocations()
            .stream()
            .filter(invocation -> invocation.getArguments().length == 1 && resumeId.equals(invocation.getArgument(0)))
            .count();
    }

    private static BackgroundJobServer createBackgroundJobServer(
        StorageProvider storageProvider, JsonMapper jsonMapper, ApplicationContext applicationContext) {

        BackgroundJobServer backgroundJobServer = new BackgroundJobServer(
            storageProvider, jsonMapper, applicationContext::getBean,
            BackgroundJobServerConfiguration.usingStandardBackgroundJobServerConfiguration()
                .andPollInterval(POLL_INTERVAL));

        backgroundJobServer.setJobFilters(List.of(new OneTimeTaskRetryFilter(), new ScheduledFireTimeFilter()));
        backgroundJobServer.start();

        return backgroundJobServer;
    }

    private Job findOneTimeTaskJob(long jobId) {
        for (int slot = 0; slot < 2; slot++) {
            try {
                return storageProvider.getJobById(JobRunrIds.jobId("one-time-task:public:" + jobId, slot));
            } catch (JobNotFoundException jobNotFoundException) {
                // try the other slot
                assertThat(slot).isLessThan(2);
            }
        }

        return null;
    }

    private Job getOneTimeTaskJob(long jobId) {
        Job job = findOneTimeTaskJob(jobId);

        assertThat(job).isNotNull();

        return job;
    }

    private List<Job> getPendingRuns(String baseId) {
        List<Job> pendingRuns = new ArrayList<>();

        for (StateName stateName : List.of(StateName.SCHEDULED, StateName.ENQUEUED)) {
            storageProvider.getJobList(stateName, org.jobrunr.storage.Paging.AmountBasedList.ascOnUpdatedAt(1000))
                .stream()
                .filter(job -> job.getRecurringJobId()
                    .map(recurringJobId -> recurringJobId.startsWith(baseId))
                    .orElse(false))
                .forEach(pendingRuns::add);
        }

        return pendingRuns;
    }

    private RecurringJob getRecurringJob(String recurringJobId) {
        return storageProvider.getRecurringJobs()
            .stream()
            .filter(recurringJob -> recurringJob.getId()
                .equals(recurringJobId))
            .findFirst()
            .orElseThrow();
    }

    private List<String> getRecurringJobIds(String baseId) {
        return storageProvider.getRecurringJobs()
            .stream()
            .map(RecurringJob::getId)
            .filter(recurringJobId -> recurringJobId.startsWith(baseId + "-g"))
            .toList();
    }

    private List<Job> getScheduledConnectionRefreshes(long connectionId) {
        List<Job> scheduledJobs = new ArrayList<>();

        for (int slot = 0; slot < 2; slot++) {
            try {
                Job job = storageProvider.getJobById(
                    JobRunrIds.jobId("connection-refresh:public:" + connectionId, slot));

                if (job.getState() == StateName.SCHEDULED) {
                    scheduledJobs.add(job);
                }
            } catch (JobNotFoundException jobNotFoundException) {
                // the slot is free
                assertThat(slot).isLessThan(2);
            }
        }

        return scheduledJobs;
    }

    private static WorkflowExecutionId workflowExecutionId(String triggerName) {
        return WorkflowExecutionId.of(
            PlatformType.AUTOMATION, 123L,
            UUID.nameUUIDFromBytes(triggerName.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .toString(),
            triggerName);
    }

    record ReceivedEvent<T>(T event, Instant receivedAt) {
    }

    static class EventCollector {

        private final Set<String> failingWorkflowExecutionIds = ConcurrentHashMap.newKeySet();
        private final List<ReceivedEvent<TriggerListenerEvent>> listenerEvents = new CopyOnWriteArrayList<>();
        private final List<ReceivedEvent<TriggerPollEvent>> pollEvents = new CopyOnWriteArrayList<>();
        private final List<ReceivedEvent<ResumeJobEvent>> resumeEvents = new CopyOnWriteArrayList<>();

        @EventListener
        public void onTriggerListenerEvent(TriggerListenerEvent triggerListenerEvent) {
            listenerEvents.add(new ReceivedEvent<>(triggerListenerEvent, Instant.now()));

            WorkflowExecutionId workflowExecutionId = triggerListenerEvent.getWorkflowExecutionId();

            if (failingWorkflowExecutionIds.contains(workflowExecutionId.toString())) {
                throw new IllegalStateException("Failing on purpose for " + workflowExecutionId.getTriggerName());
            }
        }

        @EventListener
        public void onTriggerPollEvent(TriggerPollEvent triggerPollEvent) {
            pollEvents.add(new ReceivedEvent<>(triggerPollEvent, Instant.now()));
        }

        @EventListener
        public void onResumeJobEvent(ResumeJobEvent resumeJobEvent) {
            resumeEvents.add(new ReceivedEvent<>(resumeJobEvent, Instant.now()));
        }

        void clear() {
            failingWorkflowExecutionIds.clear();
            listenerEvents.clear();
            pollEvents.clear();
            resumeEvents.clear();
        }

        void failFor(WorkflowExecutionId workflowExecutionId) {
            failingWorkflowExecutionIds.add(workflowExecutionId.toString());
        }

        List<ReceivedEvent<TriggerListenerEvent>> listenerEvents(WorkflowExecutionId workflowExecutionId) {
            return listenerEvents.stream()
                .filter(receivedEvent -> Objects.equals(workflowExecutionId.toString(), receivedEvent.event()
                    .getWorkflowExecutionId()
                    .toString()))
                .toList();
        }

        int pollEvents(WorkflowExecutionId workflowExecutionId) {
            return (int) pollEvents.stream()
                .filter(receivedEvent -> Objects.equals(workflowExecutionId.toString(), receivedEvent.event()
                    .getWorkflowExecutionId()
                    .toString()))
                .count();
        }

        int resumeEvents(long jobId) {
            return (int) resumeEvents.stream()
                .filter(receivedEvent -> receivedEvent.event()
                    .getJobId() == jobId)
                .count();
        }

        Instant resumeReceivedAt(long jobId) {
            return resumeEvents.stream()
                .filter(receivedEvent -> receivedEvent.event()
                    .getJobId() == jobId)
                .map(ReceivedEvent::receivedAt)
                .findFirst()
                .orElseThrow();
        }
    }

    @TestConfiguration
    static class JobRunrTriggerSchedulerIntTestConfiguration {

        @Bean
        ApplicationProperties applicationProperties() {
            ApplicationProperties applicationProperties = new ApplicationProperties();

            ApplicationProperties.Coordinator.Trigger.Polling polling = applicationProperties.getCoordinator()
                .getTrigger()
                .getPolling();

            polling.setCheckPeriod(5);

            return applicationProperties;
        }

        @Bean
        ConnectionFacade connectionFacade() {
            return mock(ConnectionFacade.class);
        }

        @Bean
        EventCollector eventCollector() {
            return new EventCollector();
        }

        @Bean
        JobResumeFacade jobResumeFacade() {
            JobResumeFacade jobResumeFacade = mock(JobResumeFacade.class);

            when(jobResumeFacade.resumeExpiredJob(anyString())).thenReturn(JobResumeOutcome.OK);

            return jobResumeFacade;
        }

        @Bean(destroyMethod = "stop")
        BackgroundJobServer testBackgroundJobServer(
            ApplicationContext applicationContext, JsonMapper jobRunrJsonMapper, StorageProvider storageProvider) {

            return createBackgroundJobServer(storageProvider, jobRunrJsonMapper, applicationContext);
        }
    }
}
