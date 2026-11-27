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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bytechef.config.ApplicationProperties;
import com.bytechef.platform.scheduler.ConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.QuartzConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.QuartzTriggerScheduler;
import com.bytechef.platform.scheduler.TriggerScheduler;
import com.bytechef.platform.scheduler.config.QuartzSchedulerConfiguration;
import com.bytechef.platform.scheduler.jobrunr.JobRunrConnectionRefreshScheduler;
import com.bytechef.platform.scheduler.jobrunr.JobRunrStripeUsageReportScheduler;
import com.bytechef.platform.scheduler.jobrunr.JobRunrTriggerScheduler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.jobrunr.dashboard.JobRunrDashboardWebServer;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.server.BackgroundJobServer;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Proves that the provider property alone decides which scheduler exists: with {@code quartz} (or no value) nothing of
 * JobRunr is created, not even its tables; with {@code jobrunr} the JobRunr implementations replace the Quartz ones.
 *
 * @author Nicholas D'hondt
 */
class JobRunrSchedulerConfigurationTest {

    private final ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner()
        .withUserConfiguration(JobRunrSchedulerConfiguration.class, QuartzSchedulerConfiguration.class)
        .withBean(ApplicationProperties.class, ApplicationProperties::new)
        .withBean(Scheduler.class, () -> mock(Scheduler.class));

    @Test
    void testQuartzProviderCreatesNoJobRunrBeanAndNoJobRunrTable() {
        DataSource dataSource = createDataSource();

        applicationContextRunner.withBean(DataSource.class, () -> dataSource)
            .withPropertyValues("bytechef.scheduler.provider=quartz")
            .run(context -> {
                assertThat(context).hasNotFailed()
                    .doesNotHaveBean(StorageProvider.class)
                    .doesNotHaveBean(JobScheduler.class)
                    .doesNotHaveBean(BackgroundJobServer.class)
                    .doesNotHaveBean(JobRunrSchedulerConfiguration.class);
                assertThat(context.getBean(TriggerScheduler.class)).isInstanceOf(QuartzTriggerScheduler.class);
                assertThat(context.getBean(ConnectionRefreshScheduler.class))
                    .isInstanceOf(QuartzConnectionRefreshScheduler.class);
            });

        assertThat(getJobRunrTables(dataSource)).isEmpty();
    }

    @Test
    void testMissingProviderMeansQuartz() {
        applicationContextRunner.withBean(DataSource.class, JobRunrSchedulerConfigurationTest::createDataSource)
            .run(context -> {
                assertThat(context).doesNotHaveBean(StorageProvider.class);
                assertThat(context.getBean(TriggerScheduler.class)).isInstanceOf(QuartzTriggerScheduler.class);
            });
    }

    @Test
    void testJobRunrProviderReplacesTheQuartzSchedulers() {
        DataSource dataSource = createDataSource();

        applicationContextRunner.withBean(DataSource.class, () -> dataSource)
            .withPropertyValues("bytechef.scheduler.provider=jobrunr")
            .run(context -> {
                assertThat(context).hasNotFailed()
                    .hasSingleBean(StorageProvider.class)
                    .hasSingleBean(JobScheduler.class)
                    .hasSingleBean(BackgroundJobServer.class)
                    .doesNotHaveBean(JobRunrDashboardWebServer.class)
                    .doesNotHaveBean(JobRunrStripeUsageReportScheduler.class)
                    .doesNotHaveBean(QuartzSchedulerConfiguration.class);
                assertThat(context.getBean(TriggerScheduler.class)).isInstanceOf(JobRunrTriggerScheduler.class);
                assertThat(context.getBean(ConnectionRefreshScheduler.class))
                    .isInstanceOf(JobRunrConnectionRefreshScheduler.class);
            });

        assertThat(getJobRunrTables(dataSource)).contains("JOBRUNR_JOBS", "JOBRUNR_RECURRING_JOBS");
    }

    @Test
    void testJobRunrProviderRunsNoBackgroundJobServerWhenCoordinatorIsDisabled() {
        applicationContextRunner.withBean(DataSource.class, JobRunrSchedulerConfigurationTest::createDataSource)
            .withPropertyValues("bytechef.scheduler.provider=jobrunr", "bytechef.coordinator.enabled=false")
            .run(context -> assertThat(context).hasNotFailed()
                .hasSingleBean(JobScheduler.class)
                .hasSingleBean(TriggerScheduler.class)
                .doesNotHaveBean(BackgroundJobServer.class));
    }

    @Test
    @SuppressFBWarnings(value = "UNENCRYPTED_SERVER_SOCKET", justification = "only finds a free port for the dashboard")
    void testJobRunrProviderStartsTheDashboardWhenEnabled() throws IOException {
        ApplicationProperties applicationProperties = new ApplicationProperties();

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            applicationProperties.getScheduler()
                .getJobrunr()
                .getDashboard()
                .setPort(serverSocket.getLocalPort());
        }

        new ApplicationContextRunner()
            .withUserConfiguration(JobRunrSchedulerConfiguration.class)
            .withBean(ApplicationProperties.class, () -> applicationProperties)
            .withBean(DataSource.class, JobRunrSchedulerConfigurationTest::createDataSource)
            .withPropertyValues("bytechef.scheduler.provider=jobrunr",
                "bytechef.scheduler.jobrunr.dashboard.enabled=true")
            .run(context -> assertThat(context).hasNotFailed()
                .hasSingleBean(JobRunrDashboardWebServer.class));
    }

    @Test
    void testJobRunrProviderSchedulesTheStripeUsageReportWhenBillingIsEnabled() {
        applicationContextRunner.withBean(DataSource.class, JobRunrSchedulerConfigurationTest::createDataSource)
            .withPropertyValues("bytechef.scheduler.provider=jobrunr", "billing.enabled=true")
            .run(context -> assertThat(context).hasNotFailed()
                .hasSingleBean(JobRunrStripeUsageReportScheduler.class));
    }

    private static DataSource createDataSource() {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource(
            "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");

        driverManagerDataSource.setDriverClassName("org.h2.Driver");

        return driverManagerDataSource;
    }

    private static List<String> getJobRunrTables(DataSource dataSource) {
        List<String> tableNames = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
            ResultSet resultSet = connection.getMetaData()
                .getTables(null, null, "JOBRUNR%", new String[] {
                    "TABLE"
                })) {

            while (resultSet.next()) {
                tableNames.add(resultSet.getString("TABLE_NAME"));
            }
        } catch (SQLException sqlException) {
            throw new IllegalStateException(sqlException);
        }

        return tableNames;
    }
}
