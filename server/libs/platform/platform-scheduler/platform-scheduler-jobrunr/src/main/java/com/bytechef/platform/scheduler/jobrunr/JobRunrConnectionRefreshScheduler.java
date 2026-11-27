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

import static com.bytechef.platform.scheduler.constant.QuartzConnectionRefreshConstant.TOKEN_REFRESH_OFFSET_MINUTES;

import com.bytechef.platform.scheduler.ConnectionRefreshScheduler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * {@link ConnectionRefreshScheduler} on JobRunr OSS: the token is refreshed {@code TOKEN_REFRESH_OFFSET_MINUTES} before
 * it expires, as in the Quartz provider.
 *
 * @author Nicholas D'hondt
 */
public class JobRunrConnectionRefreshScheduler implements ConnectionRefreshScheduler {

    static final String CONNECTION_REFRESH = "connection-refresh";

    private final JobRunrSchedules jobRunrSchedules;

    @SuppressFBWarnings("EI")
    public JobRunrConnectionRefreshScheduler(JobRunrSchedules jobRunrSchedules) {
        this.jobRunrSchedules = jobRunrSchedules;
    }

    @Override
    public void cancelConnectionRefresh(Long connectionId, String tenantId) {
        jobRunrSchedules.cancelOnce(getKey(connectionId, tenantId));
    }

    @Override
    public void scheduleConnectionRefresh(Long connectionId, Instant tokenExpirationTime, String tenantId) {
        long connectionIdValue = connectionId;

        jobRunrSchedules.scheduleOnce(
            getKey(connectionId, tenantId),
            tokenExpirationTime.minus(Duration.ofMinutes(TOKEN_REFRESH_OFFSET_MINUTES)),
            "OAuth2 token refresh of connection %s".formatted(connectionId),
            List.of(CONNECTION_REFRESH, "tenant:" + tenantId),
            jobs -> jobs.executeConnectionRefresh(connectionIdValue, tenantId));
    }

    private static String getKey(Long connectionId, String tenantId) {
        return CONNECTION_REFRESH + ":" + tenantId + ":" + connectionId;
    }
}
