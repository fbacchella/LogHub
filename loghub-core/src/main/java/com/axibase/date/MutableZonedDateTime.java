package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class MutableZonedDateTime extends MutableDateTime {
	private ZoneId zoneId;

	public MutableZonedDateTime ofZonedDateTime(ZonedDateTime dateTime) {
		ofLocalDateTime(dateTime.toLocalDateTime());
		this.zoneId = dateTime.getZone();
		return this;
	}

	@Override
	public MutableZonedDateTime ofInstant(Instant instant, ZoneId zoneId) {
		super.ofInstant(instant, zoneId);
		this.zoneId = zoneId;
		return this;
	}

	@Override
	public MutableZonedDateTime ofEpochSecond(long epochSecond, int nanoOfSecond, ZoneOffset offset) {
		super.ofEpochSecond(epochSecond, nanoOfSecond, offset);
		this.zoneId = offset;
		return this;
	}

	public ZoneId getZoneId() {
		return zoneId;
	}
}
