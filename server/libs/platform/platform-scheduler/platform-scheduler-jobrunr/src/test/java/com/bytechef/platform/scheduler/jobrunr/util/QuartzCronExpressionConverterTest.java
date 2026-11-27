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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.jobrunr.scheduling.cron.CronExpression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Feeds the patterns the schedule components build, and typical expressions typed into the cron trigger, to Quartz's
 * {@link org.quartz.CronExpression} and to JobRunr's {@link CronExpression} after conversion, and compares the next
 * fire times in several time zones.
 *
 * @author Nicholas D'hondt
 */
class QuartzCronExpressionConverterTest {

    private static final int FIRE_TIMES = 25;
    private static final List<ZoneId> ZONE_IDS = List.of(
        ZoneId.of("UTC"), ZoneId.of("Europe/Zagreb"), ZoneId.of("America/New_York"), ZoneId.of("Asia/Kolkata"));
    private static final List<Instant> START_INSTANTS = List.of(
        Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-03-28T22:17:41Z"),
        Instant.parse("2026-10-24T21:59:59Z"), Instant.parse("2026-12-31T23:30:00Z"));

    static Stream<String> scheduleComponentPatterns() {
        List<String> patterns = new ArrayList<>();

        for (int minute : new int[] {
            0, 7, 30, 59
        }) {
            for (int hour : new int[] {
                0, 2, 9, 23
            }) {
                // ScheduleEveryMonthTrigger
                for (int dayOfMonth : new int[] {
                    1, 15, 29, 31
                }) {
                    patterns.add("0 %s %s %s * ?".formatted(minute, hour, dayOfMonth));
                }

                // ScheduleEveryWeekTrigger
                for (int dayOfWeek = 1; dayOfWeek <= 7; dayOfWeek++) {
                    patterns.add("0 %s %s ? * %s".formatted(minute, hour, dayOfWeek));
                }

                // ScheduleEveryDayTrigger
                patterns.add("0 %s %s ? * %s".formatted(minute, hour, "1,2,3,4,5,6,7"));
                patterns.add("0 %s %s ? * %s".formatted(minute, hour, "2,3,4,5,6"));
                patterns.add("0 %s %s ? * %s".formatted(minute, hour, "1,7"));
            }
        }

        // ScheduleIntervalTrigger: minutes, hours, days, months
        for (int interval : new int[] {
            1, 2, 5, 7, 15, 30, 45
        }) {
            patterns.add("0 */%s * ? * *".formatted(interval));
        }

        for (int interval : new int[] {
            1, 2, 3, 6, 12
        }) {
            patterns.add("0 0 */%s ? * *".formatted(interval));
        }

        for (int interval : new int[] {
            1, 2, 3, 10, 15
        }) {
            patterns.add("0 0 0 */%s * ?".formatted(interval));
        }

        for (int interval = 1; interval <= 12; interval++) {
            patterns.add("0 0 0 1 */%s ?".formatted(interval));
        }

        // ScheduleCronTrigger: "0 " + what the user typed
        Stream.of(
            "* * * * ?", "*/5 * * * ?", "0 9 * * ?", "0 9 ? * MON-FRI", "30 8 ? * 2-6", "0 0 1 * ?",
            "0 12 ? * SUN", "15 10 ? * 6", "0/30 8-17 ? * MON-FRI", "0 6,18 * * ?", "0 0 ? JAN,JUL 1",
            "0 0 1-7 * ?", "5 4 ? * 1/2", "0 22 ? * 2-6", "0 0 ? * 1,7", "0 9 15 * ?", "0 9 ? * MON-SUN",
            "0 9 ? * FRI-MON", "0 9 ? * SUN-SAT", "0 9 ? * SAT-SUN", "0 9 ? * 6-2", "0 9 ? * mon-fri",
            "0 9 ? * MON,WED-FRI", "0 9 ? * 2/3")
            .map(expression -> "0 " + expression)
            .forEach(patterns::add);

        return patterns.stream();
    }

    /**
     * Outside the two days a year on which the clock changes, the converted expression fires at exactly the instants
     * Quartz fires at. On those days the two may differ; {@link #testDaylightSavingTimeInZagreb()} pins how.
     */
    @ParameterizedTest
    @MethodSource("scheduleComponentPatterns")
    void testConvertedExpressionFiresAtTheSameTimesAsQuartz(String quartzPattern) throws ParseException {
        CronExpression jobRunrCronExpression = new CronExpression(QuartzCronExpressionConverter.convert(quartzPattern));

        for (ZoneId zoneId : ZONE_IDS) {
            org.quartz.CronExpression quartzCronExpression = new org.quartz.CronExpression(quartzPattern);

            quartzCronExpression.setTimeZone(TimeZone.getTimeZone(zoneId));

            ZoneId jobRunrZoneId = QuartzCronExpressionConverter.getZoneId(quartzPattern, zoneId);

            for (Instant startInstant : START_INSTANTS) {
                List<Instant> quartzFireTimes = getQuartzFireTimes(quartzCronExpression, startInstant);
                List<Instant> jobRunrFireTimes =
                    getJobRunrFireTimes(jobRunrCronExpression, startInstant, jobRunrZoneId);
                Instant until = Collections.min(
                    List.of(quartzFireTimes.getLast(), jobRunrFireTimes.getLast()));

                assertThat(withoutDaylightSavingTimeChangeDays(jobRunrFireTimes, until, zoneId))
                    .as("%s in %s from %s", quartzPattern, zoneId, startInstant)
                    .containsExactlyElementsOf(withoutDaylightSavingTimeChangeDays(quartzFireTimes, until, zoneId));
            }
        }
    }

    /**
     * Expressions that fire every N minutes keep firing at the same pace through both daylight saving time changes: the
     * provider fires at exactly the instants of the expression evaluated at the zone's standard offset.
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "0 */5 * ? * *", "0 */15 * ? * *", "0 */30 * ? * *", "0 * * ? * *", "30 */7 * * * ?"
    })
    void testMinuteIntervalsKeepTheirPaceThroughDaylightSavingTimeChanges(String quartzPattern)
        throws ParseException {

        for (ZoneId zoneId : List.of(ZoneId.of("Europe/Zagreb"), ZoneId.of("America/New_York"))) {
            org.quartz.CronExpression standardOffsetCronExpression = new org.quartz.CronExpression(quartzPattern);

            standardOffsetCronExpression.setTimeZone(
                TimeZone.getTimeZone(zoneId.getRules()
                    .getStandardOffset(Instant.now())));

            for (String start : List.of(
                "2026-03-29T00:00:00Z", "2026-10-25T00:00:00Z", "2026-03-08T06:00:00Z", "2026-11-01T05:00:00Z")) {

                List<List<Instant>> fireTimes = getFireTimesBothWays(quartzPattern, zoneId, start, FIRE_TIMES);

                assertThat(fireTimes.get(1))
                    .as("%s in %s from %s", quartzPattern, zoneId, start)
                    .containsExactlyElementsOf(getQuartzFireTimes(standardOffsetCronExpression, Instant.parse(start)));
            }
        }
    }

    /**
     * Quartz loses the first pass of the repeated hour when the clock goes back: every 5 minutes from 02:00 CEST, the
     * next fire is 02:05 CET, an hour later. The provider fires at 02:05 CEST.
     */
    @Test
    void testQuartzSkipsTheFirstPassOfTheRepeatedHour() throws ParseException {
        List<List<Instant>> fireTimes = getFireTimesBothWays(
            "0 */5 * ? * *", ZoneId.of("Europe/Zagreb"), "2026-10-25T00:00:00Z", 2);

        assertThat(fireTimes.get(0)).containsExactly(
            Instant.parse("2026-10-25T01:05:00Z"), Instant.parse("2026-10-25T01:10:00Z"));
        assertThat(fireTimes.get(1)).containsExactly(
            Instant.parse("2026-10-25T00:05:00Z"), Instant.parse("2026-10-25T00:10:00Z"));
    }

    @Test
    void testConvertShiftsDayOfWeekNumbers() {
        assertThat(QuartzCronExpressionConverter.convert("0 0 9 ? * 2-6")).isEqualTo("0 0 9 * * 1-5");
        assertThat(QuartzCronExpressionConverter.convert("0 0 9 ? * 1,7")).isEqualTo("0 0 9 * * 0,6");
        assertThat(QuartzCronExpressionConverter.convert("0 0 9 ? * 1/2")).isEqualTo("0 0 9 * * 0/2");
        assertThat(QuartzCronExpressionConverter.convert("0 0 9 ? * MON-FRI")).isEqualTo("0 0 9 * * 1-5");
        assertThat(QuartzCronExpressionConverter.convert("0 0 9 ? * MON-SUN")).isEqualTo("0 0 9 * * 1-6,0-0");
        assertThat(QuartzCronExpressionConverter.convert("0 0 9 ? * FRI-MON")).isEqualTo("0 0 9 * * 5-6,0-1");
        assertThat(QuartzCronExpressionConverter.convert("0 0 0 */3 * ?")).isEqualTo("0 0 0 */3 * *");
        assertThat(QuartzCronExpressionConverter.convert("0 0 0 1 */2 ? *")).isEqualTo("0 0 0 1 */2 *");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "0 0 0 L * ?", "0 0 0 15W * ?", "0 0 0 LW * ?", "0 0 0 L-3 * ?", "0 0 0 ? * 6L", "0 0 0 ? * 6#3",
        "0 0 0 1 1 ? 2027", "0 0 0 * *", "0 0 0 * * ? * *", "0 0 0 ? * FRI-MON/2", "0 0 0 ? * MON/2"
    })
    void testConvertRefusesExpressionsJobRunrOssCannotRepresent(String quartzPattern) {
        assertThatThrownBy(() -> QuartzCronExpressionConverter.convert(quartzPattern))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(quartzPattern);
    }

    /**
     * Daylight saving time in Europe/Zagreb: on 29 March 2026 the clock jumps from 02:00 to 03:00, on 25 October 2026
     * it goes back from 03:00 to 02:00. Pins what each scheduler does with triggers whose local time falls inside the
     * skipped or the repeated hour. The first list is Quartz, the second JobRunr.
     */
    @Test
    void testDaylightSavingTimeInZagreb() throws ParseException {
        ZoneId zagreb = ZoneId.of("Europe/Zagreb");

        // Daily at 02:30, which does not exist on 29 March: Quartz skips that day, JobRunr runs at 03:30 local time.
        assertThat(getFireTimesBothWays("0 30 2 ? * *", zagreb, "2026-03-28T00:00:00Z", 3))
            .containsExactly(
                List.of(Instant.parse("2026-03-28T01:30:00Z"), Instant.parse("2026-03-30T00:30:00Z"),
                    Instant.parse("2026-03-31T00:30:00Z")),
                List.of(Instant.parse("2026-03-28T01:30:00Z"), Instant.parse("2026-03-29T01:30:00Z"),
                    Instant.parse("2026-03-30T00:30:00Z")));

        // Daily at 02:30, which happens twice on 25 October: both fire once; Quartz at the second 02:30 (CET),
        // JobRunr at the first (CEST).
        assertThat(getFireTimesBothWays("0 30 2 ? * *", zagreb, "2026-10-24T00:00:00Z", 3))
            .containsExactly(
                List.of(Instant.parse("2026-10-24T00:30:00Z"), Instant.parse("2026-10-25T01:30:00Z"),
                    Instant.parse("2026-10-26T01:30:00Z")),
                List.of(Instant.parse("2026-10-24T00:30:00Z"), Instant.parse("2026-10-25T00:30:00Z"),
                    Instant.parse("2026-10-26T01:30:00Z")));

        // Every hour on 25 October: both fire at 02:00 once; Quartz at the second (CET), JobRunr at the first (CEST).
        assertThat(getFireTimesBothWays("0 0 */1 ? * *", zagreb, "2026-10-24T22:30:00Z", 4))
            .containsExactly(
                List.of(Instant.parse("2026-10-24T23:00:00Z"), Instant.parse("2026-10-25T01:00:00Z"),
                    Instant.parse("2026-10-25T02:00:00Z"), Instant.parse("2026-10-25T03:00:00Z")),
                List.of(Instant.parse("2026-10-24T23:00:00Z"), Instant.parse("2026-10-25T00:00:00Z"),
                    Instant.parse("2026-10-25T02:00:00Z"), Instant.parse("2026-10-25T03:00:00Z")));

        // Every 2 hours on 29 March: 02:00 does not exist; Quartz skips it, JobRunr runs it at 03:00 local time.
        assertThat(getFireTimesBothWays("0 0 */2 ? * *", zagreb, "2026-03-28T22:30:00Z", 3))
            .containsExactly(
                List.of(Instant.parse("2026-03-28T23:00:00Z"), Instant.parse("2026-03-29T02:00:00Z"),
                    Instant.parse("2026-03-29T04:00:00Z")),
                List.of(Instant.parse("2026-03-28T23:00:00Z"), Instant.parse("2026-03-29T01:00:00Z"),
                    Instant.parse("2026-03-29T02:00:00Z")));
    }

    private static List<Instant> withoutDaylightSavingTimeChangeDays(
        List<Instant> fireTimes, Instant until, ZoneId zoneId) {

        ZoneRules zoneRules = zoneId.getRules();

        return fireTimes.stream()
            .filter(fireTime -> !fireTime.isAfter(until))
            .filter(fireTime -> {
                LocalDate localDate = LocalDate.ofInstant(fireTime, zoneId);
                ZoneOffsetTransition zoneOffsetTransition = zoneRules.nextTransition(
                    localDate.atStartOfDay(zoneId)
                        .toInstant()
                        .minusSeconds(1));

                return zoneOffsetTransition == null || !LocalDate.ofInstant(zoneOffsetTransition.getInstant(), zoneId)
                    .equals(localDate);
            })
            .toList();
    }

    private static List<List<Instant>> getFireTimesBothWays(
        String quartzPattern, ZoneId zoneId, String start, int count) throws ParseException {

        org.quartz.CronExpression quartzCronExpression = new org.quartz.CronExpression(quartzPattern);

        quartzCronExpression.setTimeZone(TimeZone.getTimeZone(zoneId));

        CronExpression jobRunrCronExpression = new CronExpression(QuartzCronExpressionConverter.convert(quartzPattern));

        Instant startInstant = Instant.parse(start);

        return List.of(
            getQuartzFireTimes(quartzCronExpression, startInstant).subList(0, count),
            getJobRunrFireTimes(
                jobRunrCronExpression, startInstant, QuartzCronExpressionConverter.getZoneId(quartzPattern, zoneId))
                    .subList(0, count));
    }

    private static List<Instant> getJobRunrFireTimes(
        CronExpression cronExpression, Instant startInstant, ZoneId zoneId) {

        List<Instant> fireTimes = new ArrayList<>();
        Instant current = startInstant;

        for (int index = 0; index < FIRE_TIMES; index++) {
            current = cronExpression.next(startInstant, current, zoneId);

            fireTimes.add(current);
        }

        return fireTimes;
    }

    private static List<Instant> getQuartzFireTimes(org.quartz.CronExpression cronExpression, Instant startInstant) {
        List<Instant> fireTimes = new ArrayList<>();
        Date current = Date.from(startInstant);

        for (int index = 0; index < FIRE_TIMES; index++) {
            current = cronExpression.getNextValidTimeAfter(current);

            fireTimes.add(current.toInstant());
        }

        return fireTimes;
    }
}
