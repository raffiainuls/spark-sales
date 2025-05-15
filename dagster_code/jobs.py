import subprocess
import os
from dagster import op, job, In, Nothing

# Jalankan dari root agar path ./data tetap konsisten
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

@op
def run_etl_1() -> Nothing:
    subprocess.run(["python", "-m", "etl.sum_transactions.sum_transactions"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_2() -> Nothing:
    subprocess.run(["python", "-m", "etl.product_performance.product_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_3() -> Nothing:
    subprocess.run(["python", "-m", "etl.branch_performance.branch_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_4() -> Nothing:
    subprocess.run(["python", "-m", "etl.monthly_branch_performance.monthly_branch_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_5() -> Nothing:
    subprocess.run(["python", "-m", "etl.monthly_finance_performance.monthly_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_6() -> Nothing:
    subprocess.run(["python", "-m", "etl.weeakly_finance_performance.weeakly_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_7() -> Nothing:
    subprocess.run(["python", "-m", "etl.daily_finance_performance.daily_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_8() -> Nothing:
    subprocess.run(["python", "-m", "etl.branch_finance_performance.branch_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_9() -> Nothing:
    subprocess.run(["python", "-m", "etl.branch_monthly_finance_performance.branch_monthly_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_10() -> Nothing:
    subprocess.run(["python", "-m", "etl.branch_weeakly_finance_performance.branch_weeakly_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_11() -> Nothing:
    subprocess.run(["python", "-m", "etl.branch_daily_finance_performance.branch_daily_finance_performance"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_12() -> Nothing:
    subprocess.run(["python", "-m", "etl.customers_retention.customers_retention"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_13() -> Nothing:
    subprocess.run(["python", "-m", "etl.fact_employee.fact_employee"], cwd=PROJECT_ROOT, check=True)

@op(ins={"start": In(Nothing)})
def run_etl_14() -> Nothing:
    subprocess.run(["python", "-m", "etl.finance_performance.finance_performance"], cwd=PROJECT_ROOT, check=True)

@job
def etl_job():
    step1 = run_etl_1()
    step2 = run_etl_2(start=step1)
    step3 = run_etl_3(start=step2)
    step4 = run_etl_4(start=step3)
    step5 = run_etl_5(start=step4)
    step6 = run_etl_6(start=step5)
    step7 = run_etl_7(start=step6)
    step8 = run_etl_8(start=step7)
    step9 = run_etl_9(start=step8)
    step10 = run_etl_10(start=step9)
    step11 = run_etl_11(start=step10)
    step12 = run_etl_12(start=step11)
    step13 = run_etl_13(start=step12)
    run_etl_14(start=step13)
