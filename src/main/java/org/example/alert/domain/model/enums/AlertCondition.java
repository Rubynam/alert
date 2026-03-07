package org.example.alert.domain.model.enums;

/**
 * Alert condition types for price matching
 */
public enum AlertCondition {
    ABOVE,          // Price is above target
    BELOW,          // Price is below target
    CROSS_ABOVE,    // Price crosses above target
    CROSS_BELOW     // Price crosses below target
}