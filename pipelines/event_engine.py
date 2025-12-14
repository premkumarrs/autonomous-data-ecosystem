def trigger_events(drift_detected, rule_violations):
    print("\n========== EVENT ENGINE ==========")

    if drift_detected:
        print("⚠️ Drift detected → triggering retraining")

    if rule_violations:
        print("🚨 Rule violations → blocking downstream pipeline")

    if not drift_detected and not rule_violations:
        print("✅ System healthy → proceeding normally")
