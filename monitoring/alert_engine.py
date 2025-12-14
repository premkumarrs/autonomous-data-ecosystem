def run_alerts(quality_score, mae, config):
    alerts_enabled = config.get("alerts", {}).get("enabled", True)

    if not alerts_enabled:
        return

    if quality_score < 80:
        print("🚨 ALERT: Data quality dropped below threshold")

    if mae > 50000:
        print("🚨 ALERT: Model MAE exceeded threshold")
