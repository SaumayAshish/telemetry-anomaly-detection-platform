package com.telemetry.platform.streams;

import java.util.Optional;
import com.telemetry.platform.events.AnomalyEvent;

public record AnomalyDecision (
        RollingStats newStats,
        Optional<AnomalyEvent> event,
        boolean rebaselined
) {}
