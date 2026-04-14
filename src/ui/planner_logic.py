import logging

logger = logging.getLogger(__name__)

def validate_plan_row(row, real_pks):
    """Proverava status jedne kolone u planu."""
    valid_strategies = ["keep", "hash", "mask", "mapping", "noise", "date_shift", "null", "faker_name", "faker_email", "faker_phone"]
    pk_strategies = ["keep", "hash"]

    col_name = str(row.get('column'))
    strategy = str(row.get('strategy', '')).lower().strip()

    if col_name in real_pks:
        return "✅ OK" if strategy in pk_strategies else "❌ PK: MUST BE KEEP/HASH"

    return "✅ OK" if strategy in valid_strategies else "❌ MISSING"

def calculate_privacy_score(plan_data):
    """Računa procenat zaštite podataka."""
    if not plan_data: return 0

    high_protection = ['mapping', 'hash', 'null', 'faker_name', 'faker_email', 'faker_phone']
    mid_protection = ['mask', 'noise', 'date_shift']

    score_points = sum(
        100 if str(col.get('strategy','')).lower() in high_protection
        else 50 if str(col.get('strategy','')).lower() in mid_protection
        else 0
        for col in plan_data
    )
    return min(int(score_points / len(plan_data)), 100)

def get_clean_plan(raw_plan):
    """Čisti plan od UI kolona pre slanja u bazu ili engine."""
    if isinstance(raw_plan, str):
        import json
        raw_plan = json.loads(raw_plan)

    return [{k: v for k, v in row.items() if k != 'status'} for row in raw_plan if isinstance(row, dict)]