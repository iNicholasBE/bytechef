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

package com.bytechef.platform.scheduler.jobrunr.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.bytechef.platform.constant.PlatformType;
import com.bytechef.platform.workflow.WorkflowExecutionId;
import java.time.ZoneId;
import java.util.List;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.scheduling.cron.CronExpression;
import org.junit.jupiter.api.Test;

/**
 * @author Nicholas D'hondt
 */
class JobRunrIdsTest {

    @Test
    void testRecurringJobBaseIdIsAValidRecurringJobIdForLongKeys() {
        String longTriggerName = "trigger_with_a_rather_long_name_".repeat(20);
        WorkflowExecutionId workflowExecutionId = WorkflowExecutionId.of(
            PlatformType.EMBEDDED, Long.MAX_VALUE, "8b5d0c2e-6f7a-4d8e-9a1b-2c3d4e5f6a7b",
            longTriggerName);

        String baseId = JobRunrIds.recurringJobBaseId("schedule-trigger", workflowExecutionId.toString());
        String generationId = baseId + "-g" + Long.MAX_VALUE;

        assertThat(generationId).hasSizeLessThan(128)
            .matches("[\\dA-Za-z-_(),.]+");
        assertThatCode(
            () -> new RecurringJob(
                generationId, new JobDetails("Example", null, "run", List.of()), new CronExpression("0 0 9 * * *"),
                ZoneId.of("UTC"), RecurringJob.CreatedBy.API))
                    .doesNotThrowAnyException();
    }

    @Test
    void testIdsAreDeterministicAndDifferPerKeyAndSlot() {
        assertThat(JobRunrIds.recurringJobBaseId("polling-trigger", "abc"))
            .isEqualTo(JobRunrIds.recurringJobBaseId("polling-trigger", "abc"))
            .isNotEqualTo(JobRunrIds.recurringJobBaseId("polling-trigger", "abd"))
            .isNotEqualTo(JobRunrIds.recurringJobBaseId("schedule-trigger", "abc"));
        assertThat(JobRunrIds.jobId("one-time-task:public:1", 0))
            .isEqualTo(JobRunrIds.jobId("one-time-task:public:1", 0))
            .isNotEqualTo(JobRunrIds.jobId("one-time-task:public:1", 1))
            .isNotEqualTo(JobRunrIds.jobId("one-time-task:other:1", 0));
    }
}
