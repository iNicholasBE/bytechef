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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Derives JobRunr ids from ByteChef scheduling keys. Both are deterministic, so a cancel finds what a schedule created
 * without a lookup table.
 *
 * <p>
 * Recurring job ids may only contain {@code [A-Za-z0-9-_(),.]} and must stay under 128 characters, while a
 * {@code WorkflowExecutionId} string is Base64 of arbitrary length; the id therefore carries a SHA-256 digest of the
 * key behind a readable prefix.
 *
 * @author Nicholas D'hondt
 */
public final class JobRunrIds {

    private JobRunrIds() {
    }

    /**
     * @return a recurring job id prefix such as {@code schedule-trigger-0c7f...} (49 characters for that prefix)
     */
    public static String recurringJobBaseId(String prefix, String key) {
        return prefix + "-" + HexFormat.of()
            .formatHex(sha256(key), 0, 16);
    }

    /**
     * @return a UUID built from the first 16 bytes of the SHA-256 digest of the key and the slot
     */
    public static UUID jobId(String key, int slot) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(sha256(key + "#" + slot));

        return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    }

    private static byte[] sha256(String value) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");

            return messageDigest.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
            throw new IllegalStateException(noSuchAlgorithmException);
        }
    }
}
