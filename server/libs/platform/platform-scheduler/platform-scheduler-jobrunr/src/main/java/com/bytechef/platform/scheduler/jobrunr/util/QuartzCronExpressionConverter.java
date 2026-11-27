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

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts the Quartz cron expressions that {@code TriggerScheduler#scheduleScheduleTrigger} receives into JobRunr cron
 * expressions. The schedule components build Quartz patterns (seconds first, {@code ?} placeholders, day-of-week
 * numbered 1 = Sunday) and the cron trigger passes what the user typed, so the conversion happens at runtime.
 *
 * <p>
 * JobRunr OSS has no equivalent for {@code L}, {@code W}, {@code #} or a concrete year. Such an expression is refused
 * with an {@link IllegalArgumentException} instead of being scheduled as something the user did not ask for.
 *
 * @author Nicholas D'hondt
 */
public final class QuartzCronExpressionConverter {

    private static final Duration DAYLIGHT_SAVING_TIME_LOOKAHEAD = Duration.ofDays(2 * 366);
    private static final Pattern DAY_OF_WEEK_NUMBER = Pattern.compile("(?<![/\\d])([1-7])(?!\\d)");
    private static final Pattern UNSUPPORTED_CHARACTERS = Pattern.compile("[LW#]");

    private static final Set<String> WILDCARDS = Set.of("*", "?");

    private QuartzCronExpressionConverter() {
    }

    /**
     * @param quartzExpression a Quartz cron expression with 6 or 7 fields
     * @return the equivalent JobRunr cron expression with 6 fields (seconds first)
     * @throws IllegalArgumentException when the expression cannot be represented in JobRunr OSS
     */
    public static String convert(String quartzExpression) {
        String[] fields = quartzExpression.trim()
            .split("\\s+");

        if (fields.length < 6 || fields.length > 7) {
            throw new IllegalArgumentException(
                "Expected a cron expression with 6 or 7 fields (seconds first): " + quartzExpression);
        }

        if (fields.length == 7 && !fields[6].equals("*") && !fields[6].equals("?")) {
            throw new IllegalArgumentException(
                "The year field is not supported by the JobRunr scheduler provider: " + quartzExpression);
        }

        String dayOfMonth = fields[3];
        String dayOfWeek = fields[5].toUpperCase(Locale.ROOT);

        if (UNSUPPORTED_CHARACTERS.matcher(dayOfMonth.toUpperCase(Locale.ROOT))
            .find() ||
            UNSUPPORTED_CHARACTERS.matcher(dayOfWeek)
                .find()) {

            throw new IllegalArgumentException(
                "'L', 'W' and '#' are not supported by the JobRunr scheduler provider: " + quartzExpression);
        }

        return String.join(
            " ", fields[0], fields[1], fields[2], dayOfMonth.equals("?") ? "*" : dayOfMonth, fields[4],
            dayOfWeek.equals("?") ? "*" : shiftDayOfWeekNumbers(dayOfWeek));
    }

    /**
     * Returns the zone in which JobRunr should evaluate the converted expression.
     *
     * <p>
     * JobRunr computes the next run on the local date and time of the zone. When the clock goes back, the local times
     * of the repeated hour occur twice but JobRunr schedules them once, so an expression that fires every N minutes
     * stops for an hour; Quartz keeps firing. For an expression that restricts neither the hour nor the day, the zone
     * only decides the minute of the hour. When every daylight saving shift of the zone is a whole number of hours,
     * that minute is the same at the zone's standard offset, which has no shifts, so the expression is evaluated there
     * and fires at exactly the instants Quartz fires at. Every other expression keeps the zone.
     */
    public static ZoneId getZoneId(String quartzExpression, ZoneId zoneId) {
        String[] fields = quartzExpression.trim()
            .split("\\s+");

        if (fields.length < 6 || !WILDCARDS.contains(fields[2]) || !WILDCARDS.contains(fields[3]) ||
            !WILDCARDS.contains(fields[4]) || !WILDCARDS.contains(fields[5])) {

            return zoneId;
        }

        ZoneRules zoneRules = zoneId.getRules();

        if (zoneRules.isFixedOffset()) {
            return zoneId;
        }

        Instant now = Instant.now();

        if (!hasWholeHourShifts(zoneRules, now)) {
            return zoneId;
        }

        ZoneOffset standardOffset = zoneRules.getStandardOffset(now);

        return standardOffset;
    }

    private static boolean hasWholeHourShifts(ZoneRules zoneRules, Instant from) {
        Instant until = from.plus(DAYLIGHT_SAVING_TIME_LOOKAHEAD);
        ZoneOffsetTransition zoneOffsetTransition = zoneRules.nextTransition(from);

        while (zoneOffsetTransition != null && zoneOffsetTransition.getInstant()
            .isBefore(until)) {

            Duration shift = zoneOffsetTransition.getDuration();

            if (shift.toSeconds() % 3600 != 0) {
                return false;
            }

            zoneOffsetTransition = zoneRules.nextTransition(zoneOffsetTransition.getInstant());
        }

        return true;
    }

    /**
     * Quartz numbers the days of the week 1 (Sunday) to 7 (Saturday), JobRunr 0 (Sunday) to 6 (Saturday). Step sizes
     * ({@code 1/2}: only the {@code 1} is a day) keep their value.
     */
    private static String shiftDayOfWeekNumbers(String dayOfWeek) {
        Matcher matcher = DAY_OF_WEEK_NUMBER.matcher(dayOfWeek);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            matcher.appendReplacement(result, String.valueOf(Integer.parseInt(matcher.group(1)) - 1));
        }

        matcher.appendTail(result);

        return result.toString();
    }
}
