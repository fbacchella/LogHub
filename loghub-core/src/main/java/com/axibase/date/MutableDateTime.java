/*
 * Copyright (c) 2007-present, Stephen Colebourne & Michael Nascimento Santos
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 *  * Neither the name of JSR-310 nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.axibase.date;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.YEAR;

public class MutableDateTime {
	private static final int SECONDS_IN_MINUTE = 60;
	private static final int SECONDS_IN_HOUR = 3600;
	private static final int SECONDS_IN_DAY = SECONDS_IN_HOUR * 24;
	private static final int YEARS_IN_LEAP_CYCLE = 400;
	private static final int DAYS_IN_LEAP_CYCLE = 146097;
	/**
	 * The number of days from year zero to year 1970.
	 * There are five 400 year cycles from year zero to 2000.
	 * There are 7 leap years from 1970 to 2000.
	 */
	private static final long DAYS_UNTIL_EPOCH = (DAYS_IN_LEAP_CYCLE * 5L) - (30L * 365L + 7L);

	private int year;
	private int monthValue;
	private int dayOfMonth;
	private int hour;
	private int minute;
	private int second;
	private int nano;

	public MutableDateTime ofInstant(Instant instant, ZoneId zoneId) {
		ZoneOffset offset = zoneId instanceof ZoneOffset ? (ZoneOffset) zoneId : zoneId.getRules().getOffset(instant);
		return ofEpochSecond(instant.getEpochSecond(), instant.getNano(), offset);
	}

	public MutableDateTime ofLocalDateTime(LocalDateTime localDateTime) {
		this.year = localDateTime.getYear();
		this.monthValue = localDateTime.getMonthValue();
		this.dayOfMonth = localDateTime.getDayOfMonth();
		this.hour = localDateTime.getHour();
		this.minute = localDateTime.getMinute();
		this.second = localDateTime.getSecond();
		this.nano = localDateTime.getNano();
		return this;
	}

	public MutableDateTime ofEpochSecond(long epochSecond, int nanoOfSecond, ZoneOffset offset) {
		Objects.requireNonNull(offset, "offset");
		NANO_OF_SECOND.checkValidValue(nanoOfSecond);
		long localSecond = epochSecond + offset.getTotalSeconds();
		long secondsInDayLong = SECONDS_IN_DAY;
		long localEpochDay = Math.floorDiv(localSecond, secondsInDayLong);
		int secsOfDay = (int)Math.floorMod(localSecond, secondsInDayLong);
		fillDateFields(localEpochDay);
		fillTimeFields(secsOfDay, nanoOfSecond);
		return this;
	}

	// implementation borrowed from ThreeTenBP
	// https://raw.githubusercontent.com/ThreeTen/threetenbp/master/src/main/java/org/threeten/bp/LocalDate.java
	private void fillDateFields(long epochDay) {
		long daysSinceZeroYear = epochDay + DAYS_UNTIL_EPOCH;
		// find the march-based year
		daysSinceZeroYear -= (1 + 31 + 28);  // adjust to 0000-03-01 so leap day is at end of four year cycle
		long adjust = 0;
		if (daysSinceZeroYear < 0) {
			// adjust negative years to positive for calculation
			long adjustCycles = (daysSinceZeroYear + 1) / DAYS_IN_LEAP_CYCLE - 1;
			adjust = adjustCycles * YEARS_IN_LEAP_CYCLE;
			daysSinceZeroYear += -adjustCycles * DAYS_IN_LEAP_CYCLE;
		}
		long yearEst = (YEARS_IN_LEAP_CYCLE * daysSinceZeroYear + 591) / DAYS_IN_LEAP_CYCLE;
		long dayOfYearEstimate = daysSinceZeroYear - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / YEARS_IN_LEAP_CYCLE);
		if (dayOfYearEstimate < 0) {
			// fix estimate
			yearEst--;
			dayOfYearEstimate = daysSinceZeroYear - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / YEARS_IN_LEAP_CYCLE);
		}
		yearEst += adjust;  // reset any negative year
		int marchBasedDayOfYear = (int) dayOfYearEstimate;

		// convert march-based values back to january-based
		int marchBasedMonth = (marchBasedDayOfYear * 5 + 2) / 153;
		yearEst += marchBasedMonth / 10;
		int month = (marchBasedMonth + 2) % 12 + 1;
		int dayOfMonth = marchBasedDayOfYear - (marchBasedMonth * (365 - 31 - 28) + 5) / 10 + 1;

		this.year = YEAR.checkValidIntValue(yearEst);
		this.monthValue = month;
		this.dayOfMonth = dayOfMonth;
	}

	private void fillTimeFields(int secondOfDay, int nanoOfSecond) {
		int hours = secondOfDay / SECONDS_IN_HOUR;
		secondOfDay -= hours * SECONDS_IN_HOUR;
		int minutes = secondOfDay / SECONDS_IN_MINUTE;
		this.hour = hours;
		this.minute = minutes;
		this.second = secondOfDay - minutes * SECONDS_IN_MINUTE;
		this.nano = nanoOfSecond;
	}

	public int getYear() {
		return year;
	}

	public int getMonthValue() {
		return monthValue;
	}

	public int getDayOfMonth() {
		return dayOfMonth;
	}

	public int getHour() {
		return hour;
	}

	public int getMinute() {
		return minute;
	}

	public int getSecond() {
		return second;
	}

	public int getNano() {
		return nano;
	}
}
