package com.telemetry.platform.alerting;

import com.telemetry.platform.alerting.AlertWriter.DeadLetter;
import com.telemetry.platform.alerting.AlertWriter.PersistResult;
import com.telemetry.platform.alerting.AlertWriter.RecordPosition;
import com.telemetry.platform.events.AnomalyEvent;
import java.sql.SQLException;

/** Where the record router sends its decisions: alerts to persist, dead letters to keep. */
interface AlertSink {

    PersistResult persist(RecordPosition position, AnomalyEvent event)
            throws SQLException, InterruptedException;

    void deadLetter(RecordPosition position, DeadLetter letter)
            throws SQLException, InterruptedException;
}
