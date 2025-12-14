import os
import json
from datetime import datetime

RULE_LOG_DIR = "logs/rule_violations"
os.makedirs(RULE_LOG_DIR, exist_ok=True)

def apply_quality_rules(df):
    violations = []

    invalid_age = df[(df["age"] < 18) | (df["age"] > 65)]
    if not invalid_age.empty:
        violations.append({
            "rule": "AGE_RANGE",
            "count": len(invalid_age),
            "message": "Age must be between 18 and 65"
        })

    invalid_salary = df[df["salary"] <= 0]
    if not invalid_salary.empty:
        violations.append({
            "rule": "SALARY_POSITIVE",
            "count": len(invalid_salary),
            "message": "Salary must be greater than 0"
        })

    missing_dept = df[df["department"].isna()]
    if not missing_dept.empty:
        violations.append({
            "rule": "DEPARTMENT_REQUIRED",
            "count": len(missing_dept),
            "message": "Department cannot be null"
        })

    return violations


def log_rule_violations(violations):
    if not violations:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(RULE_LOG_DIR, f"rule_violations_{timestamp}.json")

    with open(path, "w") as f:
        json.dump(violations, f, indent=4)

    print(f"Rule violations logged to: {path}")
