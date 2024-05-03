package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;

interface AppendOffset {
    StringBuilder append(StringBuilder sb, ZoneId offset, Instant instant);
}
