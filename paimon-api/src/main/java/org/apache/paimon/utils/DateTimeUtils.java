/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.data.Timestamp;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.TimeZone;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/** Utils for date time. */
public class DateTimeUtils {

    /** The julian date of the epoch, 1970-01-01. */
    public static final int EPOCH_JULIAN = 2440588;

    /** The number of milliseconds in a second. */
    private static final long MILLIS_PER_SECOND = 1000L;

    /** The number of milliseconds in a minute. */
    private static final long MILLIS_PER_MINUTE = 60000L;

    /** The number of milliseconds in an hour. */
    private static final long MILLIS_PER_HOUR = 3600000L; // = 60 * 60 * 1000

    /**
     * The number of milliseconds in a day.
     *
     * <p>This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
     */
    public static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

    /** The local time zone. */
    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    private static final DateTimeFormatter DEFAULT_TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-[MM][M]-[dd][d]")
                    .optionalStart()
                    .appendPattern(" [HH][H]:[mm][m]:[ss][s]")
                    .appendFraction(NANO_OF_SECOND, 0, 9, true)
                    .optionalEnd()
                    .toFormatter();

    /**
     * Converts the internal representation of a SQL DATE (int) to the Java type used for UDF
     * parameters ({@link java.sql.Date}).
     */
    public static java.sql.Date toSQLDate(int v) {
        // note that, in this case, can't handle Daylight Saving Time
        final long t = v * MILLIS_PER_DAY;
        return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
    }

    /**
     * Converts the internal representation of a SQL TIME (int) to the Java type used for UDF
     * parameters ({@link java.sql.Time}).
     */
    public static java.sql.Time toSQLTime(int v) {
        // note that, in this case, can't handle Daylight Saving Time
        return new java.sql.Time(v - LOCAL_TZ.getOffset(v));
    }

    /**
     * Converts the Java type used for UDF parameters of SQL DATE type ({@link java.sql.Date}) to
     * internal representation (int).
     */
    public static int toInternal(java.sql.Date date) {
        long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
        return (int) (ts / MILLIS_PER_DAY);
    }

    /**
     * Converts the Java type used for UDF parameters of SQL TIME type ({@link java.sql.Time}) to
     * internal representation (int).
     *
     * <p>Converse of {@link #toSQLTime(int)}.
     */
    public static int toInternal(java.sql.Time time) {
        long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
        return (int) (ts % MILLIS_PER_DAY);
    }

    public static int toInternal(LocalDate date) {
        return ymdToUnixDate(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
    }

    public static int toInternal(LocalTime time) {
        return time.getHour() * (int) MILLIS_PER_HOUR
                + time.getMinute() * (int) MILLIS_PER_MINUTE
                + time.getSecond() * (int) MILLIS_PER_SECOND
                + time.getNano() / 1000_000;
    }

    // --------------------------------------------------------------------------------------------
    // Java 8 time conversion
    // --------------------------------------------------------------------------------------------

    public static LocalDate toLocalDate(int date) {
        return julianToLocalDate(date + EPOCH_JULIAN);
    }

    private static LocalDate julianToLocalDate(int julian) {
        // this shifts the epoch back to astronomical year -4800 instead of the
        // start of the Christian era in year AD 1 of the proleptic Gregorian
        // calendar.
        int j = julian + 32044;
        int g = j / 146097;
        int dg = j % 146097;
        int c = (dg / 36524 + 1) * 3 / 4;
        int dc = dg - c * 36524;
        int b = dc / 1461;
        int db = dc % 1461;
        int a = (db / 365 + 1) * 3 / 4;
        int da = db - a * 365;

        // integer number of full years elapsed since March 1, 4801 BC
        int y = g * 400 + c * 100 + b * 4 + a;
        // integer number of full months elapsed since the last March 1
        int m = (da * 5 + 308) / 153 - 2;
        // number of days elapsed since day 1 of the month
        int d = da - (m + 4) * 153 / 5 + 122;
        int year = y - 4800 + (m + 2) / 12;
        int month = (m + 2) % 12 + 1;
        int day = d + 1;
        return LocalDate.of(year, month, day);
    }

    public static LocalTime toLocalTime(int time) {
        int h = time / 3600000;
        int time2 = time % 3600000;
        int m = time2 / 60000;
        int time3 = time2 % 60000;
        int s = time3 / 1000;
        int ms = time3 % 1000;
        return LocalTime.of(h, m, s, ms * 1000_000);
    }

    public static Integer parseDate(String s) {
        // allow timestamp str to date, e.g. 2017-12-12 09:30:00.0
        int ws1 = s.indexOf(" ");
        if (ws1 > 0) {
            s = s.substring(0, ws1);
        }
        int hyphen1 = s.indexOf('-');
        int y;
        int m;
        int d;
        if (hyphen1 < 0) {
            if (!isInteger(s.trim())) {
                return null;
            }
            y = Integer.parseInt(s.trim());
            m = 1;
            d = 1;
        } else {
            if (!isInteger(s.substring(0, hyphen1).trim())) {
                return null;
            }
            y = Integer.parseInt(s.substring(0, hyphen1).trim());
            final int hyphen2 = s.indexOf('-', hyphen1 + 1);
            if (hyphen2 < 0) {
                if (!isInteger(s.substring(hyphen1 + 1).trim())) {
                    return null;
                }
                m = Integer.parseInt(s.substring(hyphen1 + 1).trim());
                d = 1;
            } else {
                if (!isInteger(s.substring(hyphen1 + 1, hyphen2).trim())) {
                    return null;
                }
                m = Integer.parseInt(s.substring(hyphen1 + 1, hyphen2).trim());
                if (!isInteger(s.substring(hyphen2 + 1).trim())) {
                    return null;
                }
                d = Integer.parseInt(s.substring(hyphen2 + 1).trim());
            }
        }
        if (!isIllegalDate(y, m, d)) {
            return null;
        }
        return ymdToUnixDate(y, m, d);
    }

    public static Integer parseTime(String v) {
        final int start = 0;
        final int colon1 = v.indexOf(':', start);
        // timezone hh:mm:ss[.ssssss][[+|-]hh:mm:ss]
        // refer https://www.w3.org/TR/NOTE-datetime
        int timezoneHour;
        int timezoneMinute;
        int hour;
        int minute;
        int second;
        int milli;
        int operator = -1;
        int end = v.length();
        int timezone = v.indexOf('-', start);
        if (timezone < 0) {
            timezone = v.indexOf('+', start);
            operator = 1;
        }
        if (timezone < 0) {
            timezoneHour = 0;
            timezoneMinute = 0;
        } else {
            end = timezone;
            final int colon3 = v.indexOf(':', timezone);
            if (colon3 < 0) {
                if (!isInteger(v.substring(timezone + 1).trim())) {
                    return null;
                }
                timezoneHour = Integer.parseInt(v.substring(timezone + 1).trim());
                timezoneMinute = 0;
            } else {
                if (!isInteger(v.substring(timezone + 1, colon3).trim())) {
                    return null;
                }
                timezoneHour = Integer.parseInt(v.substring(timezone + 1, colon3).trim());
                if (!isInteger(v.substring(colon3 + 1).trim())) {
                    return null;
                }
                timezoneMinute = Integer.parseInt(v.substring(colon3 + 1).trim());
            }
        }
        if (colon1 < 0) {
            if (!isInteger(v.substring(start, end).trim())) {
                return null;
            }
            hour = Integer.parseInt(v.substring(start, end).trim());
            minute = 0;
            second = 0;
            milli = 0;
        } else {
            if (!isInteger(v.substring(start, colon1).trim())) {
                return null;
            }
            hour = Integer.parseInt(v.substring(start, colon1).trim());
            final int colon2 = v.indexOf(':', colon1 + 1);
            if (colon2 < 0) {
                if (!isInteger(v.substring(colon1 + 1, end).trim())) {
                    return null;
                }
                minute = Integer.parseInt(v.substring(colon1 + 1, end).trim());
                second = 0;
                milli = 0;
            } else {
                if (!isInteger(v.substring(colon1 + 1, colon2).trim())) {
                    return null;
                }
                minute = Integer.parseInt(v.substring(colon1 + 1, colon2).trim());
                int dot = v.indexOf('.', colon2);
                if (dot < 0) {
                    if (!isInteger(v.substring(colon2 + 1, end).trim())) {
                        return null;
                    }
                    second = Integer.parseInt(v.substring(colon2 + 1, end).trim());
                    milli = 0;
                } else {
                    if (!isInteger(v.substring(colon2 + 1, dot).trim())) {
                        return null;
                    }
                    second = Integer.parseInt(v.substring(colon2 + 1, dot).trim());
                    milli = parseFraction(v.substring(dot + 1, end).trim());
                }
            }
        }
        hour += operator * timezoneHour;
        minute += operator * timezoneMinute;
        return hour * (int) MILLIS_PER_HOUR
                + minute * (int) MILLIS_PER_MINUTE
                + second * (int) MILLIS_PER_SECOND
                + milli;
    }

    private static boolean isInteger(String s) {
        boolean isInt = s.length() > 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                isInt = false;
                break;
            }
        }
        return isInt;
    }

    private static boolean isIllegalDate(int y, int m, int d) {
        int[] monthOf31Days = new int[] {1, 3, 5, 7, 8, 10, 12};
        if (y < 0 || y > 9999 || m < 1 || m > 12 || d < 1 || d > 31) {
            return false;
        }
        if (m == 2 && d > 28) {
            if (!(isLeapYear(y) && d == 29)) {
                return false;
            }
        }
        if (d == 31) {
            for (int i : monthOf31Days) {
                if (i == m) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Parses a fraction, multiplying the first character by {@code multiplier}, the second
     * character by {@code multiplier / 10}, the third character by {@code multiplier / 100}, and so
     * forth.
     *
     * <p>For example, {@code parseFraction("1234", 100)} yields {@code 123}.
     */
    private static int parseFraction(String v) {
        int multiplier = 100;
        int r = 0;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            int x = c < '0' || c > '9' ? 0 : (c - '0');
            r += multiplier * x;
            if (multiplier < 10) {
                // We're at the last digit. Check for rounding.
                if (i + 1 < v.length() && v.charAt(i + 1) >= '5') {
                    ++r;
                }
                break;
            }
            multiplier /= 10;
        }
        return r;
    }

    private static boolean isLeapYear(int s) {
        return s % 400 == 0 || (s % 4 == 0 && s % 100 != 0);
    }

    private static int ymdToUnixDate(int year, int month, int day) {
        final int julian = ymdToJulian(year, month, day);
        return julian - EPOCH_JULIAN;
    }

    private static int ymdToJulian(int year, int month, int day) {
        int a = (14 - month) / 12;
        int y = year + 4800 - a;
        int m = month + 12 * a - 3;
        return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    }

    public static Timestamp parseTimestampData(String dateStr, int precision)
            throws DateTimeException {
        return Timestamp.fromLocalDateTime(
                fromTemporalAccessor(DEFAULT_TIMESTAMP_FORMATTER.parse(dateStr), precision));
    }

    /**
     * This is similar to {@link LocalDateTime#from(TemporalAccessor)}, but it's less strict and
     * introduces default values.
     */
    private static LocalDateTime fromTemporalAccessor(TemporalAccessor accessor, int precision) {
        // complement year with 1970
        int year = accessor.isSupported(YEAR) ? accessor.get(YEAR) : 1970;
        // complement month with 1
        int month = accessor.isSupported(MONTH_OF_YEAR) ? accessor.get(MONTH_OF_YEAR) : 1;
        // complement day with 1
        int day = accessor.isSupported(DAY_OF_MONTH) ? accessor.get(DAY_OF_MONTH) : 1;
        // complement hour with 0
        int hour = accessor.isSupported(HOUR_OF_DAY) ? accessor.get(HOUR_OF_DAY) : 0;
        // complement minute with 0
        int minute = accessor.isSupported(MINUTE_OF_HOUR) ? accessor.get(MINUTE_OF_HOUR) : 0;
        // complement second with 0
        int second = accessor.isSupported(SECOND_OF_MINUTE) ? accessor.get(SECOND_OF_MINUTE) : 0;
        // complement nano_of_second with 0
        int nanoOfSecond = accessor.isSupported(NANO_OF_SECOND) ? accessor.get(NANO_OF_SECOND) : 0;

        if (precision == 0) {
            nanoOfSecond = 0;
        } else if (precision != 9) {
            nanoOfSecond = (int) floor(nanoOfSecond, powerX(10, 9 - precision));
        }

        return LocalDateTime.of(year, month, day, hour, minute, second, nanoOfSecond);
    }

    private static long floor(long a, long b) {
        long r = a % b;
        if (r < 0) {
            return a - r - b;
        } else {
            return a - r;
        }
    }

    private static long powerX(long a, long b) {
        long x = 1;
        while (b > 0) {
            x *= a;
            --b;
        }
        return x;
    }

    public static Timestamp truncate(Timestamp ts, int precision) {
        String fraction = Integer.toString(ts.toLocalDateTime().getNano());
        if (fraction.length() <= precision) {
            return ts;
        } else {
            // need to truncate
            if (precision <= 3) {
                return Timestamp.fromEpochMillis(
                        zeroLastDigits(ts.getMillisecond(), 3 - precision));
            } else {
                return Timestamp.fromEpochMillis(
                        ts.getMillisecond(),
                        (int) zeroLastDigits(ts.getNanoOfMillisecond(), 9 - precision));
            }
        }
    }

    private static long zeroLastDigits(long l, int n) {
        long tenToTheN = (long) Math.pow(10, n);
        return (l / tenToTheN) * tenToTheN;
    }
}
