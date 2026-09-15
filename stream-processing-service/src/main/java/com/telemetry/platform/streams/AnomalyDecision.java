package com.telemetry.platform.streams;

import java.util.Optional;

public record AnomalyDecision (
        RollingStats newStats,
        Optional<AnomalyEvent> event,
        boolean rebaselined
) {}
