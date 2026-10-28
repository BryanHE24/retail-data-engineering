from decimal import Decimal

from src.transformations import add_line_amount


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