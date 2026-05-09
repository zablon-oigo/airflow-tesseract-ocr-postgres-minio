from typing import Dict

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


@task
def write_to_postgres(result: Dict):

    hook = PostgresHook(
        postgres_conn_id="postgres_default"
    )

    extracted = result.get("extracted", {})

    invoice_number = extracted.get(
        "invoice_number",
        ""
    )

    invoice_date = extracted.get(
        "invoice_date",
        ""
    )

    try:
        total_amount = float(
            extracted.get(
                "total_amount",
                0
            ) or 0
        )

    except (ValueError, TypeError):
        total_amount = 0.0

    sql = """
        INSERT INTO invoices (
            invoice_number,
            invoice_date,
            total_amount
        )
        VALUES (%s, %s, %s)
    """

    hook.run(
        sql,
        parameters=(
            invoice_number,
            invoice_date,
            total_amount
        )
    )

    print(
        f"Saved invoice "
        f"{invoice_number} "
        f"to PostgreSQL"
    )

    return result