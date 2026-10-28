from decimal import Decimal

from src.transformations import (
    add_line_amount, 
    normalize_invoice_number
    )


def test_line_amount_preserves_cancellations(spark):
    """Sales are positive; cancellation amounts stay negative."""

    # STEP 1: Create two synthetic records directly in Spark.
    # Use the same numeric types as our retail transformations.
    transactions = spark.sql("""
        SELECT
            example,
            CAST(quantity AS INT) AS quantity_value,
            CAST(price AS DECIMAL(18,4)) AS price_value
        FROM VALUES
            ('sale',          6, 2.55),
            ('cancellation', -2, 2.55)
        AS fixtures(example, quantity, price)
    """)

    # STEP 2: Apply our reusable PySpark transformation.
    result = add_line_amount(transactions)

    # STEP 3: Read the two amounts in a consistent order.
    amounts = sorted(
        row.line_amount
        for row in result.select("line_amount").collect()
    )

    # Confirm the sale amount and the negative cancellation amount.
    assert amounts == [Decimal("-5.10"), Decimal("15.30")]

def test_invoice_normalization(spark):
    """Normalize invoice text without hiding missing invoice numbers."""

    # STEP 1: Create labeled synthetic examples.
    transactions = spark.sql("""
        SELECT *
        FROM VALUES
            (1, ' 536365 '),
            (2, ' c536379 '),
            (3, CAST(NULL AS STRING)),
            (4, '   ')
        AS fixtures(example_id, InvoiceNo)
    """)

    # STEP 2: Apply the reusable transformation.
    result = normalize_invoice_number(transactions)

    # STEP 3: Read results in a predictable order.
    invoices = [
        row.invoice_no
        for row in result.orderBy("example_id").collect()
    ]

    # Missing and blank values remain detectable by later quality checks.
    assert invoices == ["536365", "C536379", None, ""]


