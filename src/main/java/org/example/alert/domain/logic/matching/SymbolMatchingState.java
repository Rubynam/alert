package org.example.alert.domain.logic.matching;

import lombok.Data;
import org.example.alert.domain.model.queue.AlertConfig;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Internal state maintained by SymbolMatchingActor
 */
@Data
public class SymbolMatchingState {

    private final String symbol;
    private final String source;
    private final String shardKey;  // "BINANCE:BTCUSDT"

    // Active alerts (in-memory cache)
    private final Map<String, AlertConfig> activeAlerts;

    // Previous price for cross detection
    private BigDecimal previousPrice;

    // Statistics
    private Instant lastPollTime;
    private long totalPriceEvents;
    private long totalAlertChanges;
    private long totalMatches;

    public SymbolMatchingState(String symbol, String source) {
        this.symbol = symbol;
        this.source = source;
        this.shardKey = source + ":" + symbol;
        this.activeAlerts = new ConcurrentHashMap<>();
        this.previousPrice = null;
        this.lastPollTime = Instant.now();
        this.totalPriceEvents = 0;
        this.totalAlertChanges = 0;
        this.totalMatches = 0;
    }

    public void addOrUpdateAlert(AlertConfig alert) {
        activeAlerts.put(alert.getAlertId(), alert);
        totalAlertChanges++;
    }

    public void removeAlert(String alertId) {
        activeAlerts.remove(alertId);
        totalAlertChanges++;
    }

    public int getAlertCount() {
        return activeAlerts.size();
    }
}