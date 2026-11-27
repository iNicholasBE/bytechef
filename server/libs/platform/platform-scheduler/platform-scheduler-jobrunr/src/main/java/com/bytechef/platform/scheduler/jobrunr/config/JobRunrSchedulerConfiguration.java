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

package com.bytechef.platform.scheduler.jobrunr.config;

import static org.jobrunr.dashboard.JobRunrDashboardWebServerConfiguration.usingStandardDashboardConfiguration;
import static org.jobrunr.server.BackgroundJobServerConfiguration.usingStandardBackgroundJobServerConfiguration;

import com.bytechef.atlas.configuration.service.WorkflowService;
import com.bytechef.atlas.coordinator.annotation.ConditionalOnCoordinator;
import com.bytechef.config.ApplicationProperties;
import com.bytechef.platform.billing.facade.BillingUsageFacade;
import com.bytechef.platform.component.facade.TriggerDefinitionFacade;
import com.bytechef.platform.connection.facade.ConnectionFacade;
import com.bytechef.platform.scheduler.ConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.TriggerScheduler;
import com.bytechef.platform.scheduler.jobrunr.JobRunrConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.jobrunr.JobRunrSchedules;
import com.bytechef.platform.scheduler.jobrunr.JobRunrStripeUsageReportScheduler;
import com.bytechef.platform.scheduler.jobrunr.JobRunrTriggerScheduler;
import com.bytechef.platform.scheduler.jobrunr.job.JobRunrSchedulerJobs;
import com.bytechef.platform.scheduler.jobrunr.job.OneTimeTaskRetryFilter;
import com.bytechef.platform.scheduler.jobrunr.job.ScheduledFireTimeFilter;
import com.bytechef.platform.workflow.execution.accessor.JobPrincipalAccessorRegistry;
import com.bytechef.platform.workflow.execution.facade.JobResumeFacade;
import com.bytechef.platform.workflow.execution.facade.TriggerStateFacade;
import java.util.List;
import javax.sql.DataSource;
import org.jobrunr.dashboard.JobRunrDashboardWebServer;
import org.jobrunr.dashboard.JobRunrDashboardWebServerConfiguration;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.sql.common.SqlStorageProviderFactory;
import org.jobrunr.utils.mapper.JsonMapper;
import org.jobrunr.utils.mapper.jackson3.Jackson3JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

/**
 * Wires JobRunr OSS by hand when {@code bytechef.scheduler.provider=jobrunr}. Nothing here is created under another
 * provider: no JobRunr bean, thread or table. The JobRunr Spring Boot starter is not used because it creates a storage
 * provider, and with it the JobRunr tables, as soon as a {@link DataSource} exists.
 *
 * <p>
 * Every node can schedule and cancel; only coordinator nodes run the {@link BackgroundJobServer} that executes the jobs
 * and, when enabled, the dashboard. Worker count, poll interval and thread type are JobRunr's defaults.
 *
 * @author Nicholas D'hondt
 */
@Configuration
@ConditionalOnProperty(prefix = "bytechef", name = "scheduler.provider", havingValue = "jobrunr")
public class JobRunrSchedulerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(JobRunrSchedulerConfiguration.class);

    JobRunrSchedulerConfiguration() {
        log.info("Scheduler provider type enabled: jobrunr");
    }

    /**
     * Stores the jobs in the application database. JobRunr creates and migrates its own {@code jobrunr_*} tables on
     * start.
     */
    @Bean(destroyMethod = "close")
    StorageProvider jobRunrStorageProvider(DataSource dataSource, JsonMapper jobRunrJsonMapper) {
        StorageProvider storageProvider = SqlStorageProviderFactory.using(dataSource);

        storageProvider.setJobMapper(new JobMapper(jobRunrJsonMapper));

        return storageProvider;
    }

    @Bean
    JsonMapper jobRunrJsonMapper() {
        return new Jackson3JsonMapper();
    }

    @Bean
    JobScheduler jobRunrJobScheduler(StorageProvider jobRunrStorageProvider) {
        return new JobScheduler(jobRunrStorageProvider, List.of());
    }

    @Bean
    JobRunrSchedules jobRunrSchedules(JobScheduler jobRunrJobScheduler, StorageProvider jobRunrStorageProvider) {
        return new JobRunrSchedules(jobRunrJobScheduler, jobRunrStorageProvider);
    }

    @Bean
    ConnectionRefreshScheduler jobRunrConnectionRefreshScheduler(JobRunrSchedules jobRunrSchedules) {
        return new JobRunrConnectionRefreshScheduler(jobRunrSchedules);
    }

    @Bean
    TriggerScheduler jobRunrTriggerScheduler(
        ApplicationProperties applicationProperties, JobRunrSchedules jobRunrSchedules) {

        ApplicationProperties.Coordinator.Trigger.Polling polling = applicationProperties.getCoordinator()
            .getTrigger()
            .getPolling();

        return new JobRunrTriggerScheduler(polling, jobRunrSchedules);
    }

    @Bean
    @SuppressWarnings("PMD.ExcessiveParameterList")
    JobRunrSchedulerJobs jobRunrSchedulerJobs(
        ObjectProvider<BillingUsageFacade> billingUsageFacadeProvider,
        ObjectProvider<ConnectionFacade> connectionFacadeProvider,
        ConnectionRefreshScheduler jobRunrConnectionRefreshScheduler, ApplicationEventPublisher eventPublisher,
        ObjectProvider<JobPrincipalAccessorRegistry> jobPrincipalAccessorRegistryProvider,
        ObjectProvider<JobResumeFacade> jobResumeFacadeProvider,
        ObjectProvider<TriggerDefinitionFacade> triggerDefinitionFacadeProvider,
        TriggerScheduler jobRunrTriggerScheduler, ObjectProvider<TriggerStateFacade> triggerStateFacadeProvider,
        ObjectProvider<WorkflowService> workflowServiceProvider) {

        return new JobRunrSchedulerJobs(
            billingUsageFacadeProvider, connectionFacadeProvider, jobRunrConnectionRefreshScheduler, eventPublisher,
            jobPrincipalAccessorRegistryProvider, jobResumeFacadeProvider, triggerDefinitionFacadeProvider,
            jobRunrTriggerScheduler, triggerStateFacadeProvider, workflowServiceProvider);
    }

    @Bean
    @ConditionalOnProperty(prefix = "billing", name = "enabled", havingValue = "true")
    JobRunrStripeUsageReportScheduler jobRunrStripeUsageReportScheduler(JobScheduler jobRunrJobScheduler) {
        return new JobRunrStripeUsageReportScheduler(jobRunrJobScheduler);
    }

    /**
     * Executes the jobs. Created on coordinator nodes only and started once the application is ready, as the Quartz
     * scheduler is.
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnCoordinator
    BackgroundJobServer jobRunrBackgroundJobServer(
        ApplicationContext applicationContext, JsonMapper jobRunrJsonMapper,
        StorageProvider jobRunrStorageProvider) {

        BackgroundJobServer backgroundJobServer = new BackgroundJobServer(
            jobRunrStorageProvider, jobRunrJsonMapper, applicationContext::getBean,
            usingStandardBackgroundJobServerConfiguration());

        backgroundJobServer.setJobFilters(List.of(new OneTimeTaskRetryFilter(), new ScheduledFireTimeFilter()));

        return backgroundJobServer;
    }

    @Bean
    @ConditionalOnCoordinator
    JobRunrDelayer jobRunrDelayer(BackgroundJobServer jobRunrBackgroundJobServer) {
        return new JobRunrDelayer(jobRunrBackgroundJobServer);
    }

    /**
     * The JobRunr dashboard: a separate web server, off by default. Enable it with
     * {@code bytechef.scheduler.jobrunr.dashboard.enabled=true}; set a username and password when the port is reachable
     * from outside.
     */
    @Bean(destroyMethod = "stop")
    @ConditionalOnCoordinator
    @ConditionalOnProperty(prefix = "bytechef", name = "scheduler.jobrunr.dashboard.enabled", havingValue = "true")
    JobRunrDashboardWebServer jobRunrDashboardWebServer(
        ApplicationProperties applicationProperties, JsonMapper jobRunrJsonMapper,
        StorageProvider jobRunrStorageProvider) {

        ApplicationProperties.Scheduler.JobRunr.Dashboard dashboard = applicationProperties.getScheduler()
            .getJobrunr()
            .getDashboard();

        JobRunrDashboardWebServerConfiguration dashboardWebServerConfiguration = usingStandardDashboardConfiguration()
            .andPort(dashboard.getPort());

        String username = dashboard.getUsername();

        if (username != null && !username.isEmpty()) {
            dashboardWebServerConfiguration.andBasicAuthentication(username, dashboard.getPassword());
        }

        JobRunrDashboardWebServer jobRunrDashboardWebServer = new JobRunrDashboardWebServer(
            jobRunrStorageProvider, jobRunrJsonMapper, dashboardWebServerConfiguration);

        jobRunrDashboardWebServer.start();

        return jobRunrDashboardWebServer;
    }

    record JobRunrDelayer(BackgroundJobServer backgroundJobServer) {

        @EventListener(ApplicationReadyEvent.class)
        public void startLater() {
            backgroundJobServer.start();

            log.debug("JobRunr background job server started");
        }
    }
}
