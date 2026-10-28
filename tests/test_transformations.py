from decimal import Decimal
from datetime import datetime

from src.transformations import (
    add_line_amount, 
    normalize_invoice_number,
    prepare_transaction_types,
    add_rejection_reason   
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


def test_prepare_transaction_types(spark):
    """Convert valid values and preserve invalid rows for quarantine."""

    # STEP 1: Create one valid and one invalid synthetic record.
    transactions = spark.sql("""
        SELECT *
        FROM VALUES
            (1, ' c536379 ', '12/1/10 8:26', '-2', '2.55'),
            (2, '536380', 'invalid-date', 'unknown', 'invalid-price')
        AS fixtures(example_id, InvoiceNo, InvoiceDate, Quantity, UnitPrice)
    """)

    # STEP 2: Apply the type conversions.
    result = prepare_transaction_types(transactions)

    # STEP 3: Read the tiny result in a predictable order.
    rows = result.orderBy("example_id").collect()

    # Both records must remain available for later quality checks.
    assert len(rows) == 2

    # Valid strings become the expected typed values.
    assert rows[0].invoice_no == "C536379"
    assert rows[0].invoice_timestamp == datetime(2010, 12, 1, 8, 26)
    assert rows[0].quantity_value == -2
    assert rows[0].price_value == Decimal("2.5500")

    # Invalid values become null instead of stopping the pipeline.
    assert rows[1].invoice_timestamp is None
    assert rows[1].quantity_value is None
    assert rows[1].price_value is None

    # Keep the original value so we can investigate rejected records.
    assert rows[1].Quantity == "unknown"

    def test_rejection_reasons(spark):
        """Check accepted records, rejected records, and rule priority."""

    # STEP 1: Create synthetic examples.
    # Most examples share a valid date so we can focus on other rules.
    transactions = spark.sql("""
        SELECT *
        FROM VALUES
            (1, '536365',  '12/1/10 8:26', '6',       '2.55'),
            (2, 'C536379', '12/1/10 8:26', '-2',      '2.55'),
            (3, '   ',    '12/1/10 8:26', '1',       '2.55'),
            (4, '536380',  'invalid',      '1',       '2.55'),
            (5, '536381',  '12/1/10 8:26', 'unknown', '2.55'),
            (6, '536382',  '12/1/10 8:26', '1',       'unknown'),
            (7, '536383',  '12/1/10 8:26', '1',       '0'),
            (8, '536384',  '12/1/10 8:26', '1',       '-1'),
            (9, '536385',  '12/1/10 8:26', '0',       '2.55'),
            (10, '536386', '12/1/10 8:26', '-2',      '2.55'),
            (11, 'C536387','12/1/10 8:26', '2',       '2.55'),
            (12, '536388', '12/1/10 8:26', '-2',      '0')
        AS fixtures(example_id, InvoiceNo, InvoiceDate, Quantity, UnitPrice)
    """)

    # STEP 2: Convert the strings, then apply the quality rules.
    typed = prepare_transaction_types(transactions)
    checked = add_rejection_reason(typed)

    # STEP 3: Read reasons in the same order as our examples.
    reasons = [
        row.rejection_reason
        for row in checked.orderBy("example_id").collect()
    ]

    # Check every outcome, including which rule takes priority.
    assert reasons == [
        None,                                      # Valid sale.
        None,                                      # Valid cancellation.
        "missing_invoice",
        "invalid_date",
        "invalid_quantity",
        "invalid_price",
        "non_positive_price",                      # Zero price.
        "non_positive_price",                      # Negative price.
        "zero_quantity",
        "negative_quantity_without_cancellation",
        "cancellation_with_positive_quantity",
        "non_positive_price",                      # Price rule comes first.
    ]