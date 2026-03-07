package org.example.alert.domain.model.enums;

/**
 * Frequency condition for alert triggering
 */
public enum FrequencyCondition {
    ONLY_ONCE,          // Trigger only once
    PER_DAY,            // Trigger once per day
    ALWAYS_PER_MINUTE   // Trigger continuously (max once per minute)
}