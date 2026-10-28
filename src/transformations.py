from pyspark.sql.functions import col, expr, trim, upper


def add_line_amount(transactions):
    """
        Add the amount for each transaction using its typed numeric columns.
        Receives a DataFrame called transactions.
        Calculates line_amount.
        Returns a new DataFrame without writing a table.
    """

    # Keep all existing columns and add the calculated amount.
    # Preserve  the quantity's sign so cancellations stay negative.
    # Multiply quantity by price, preserving negative cancellations.
    return transactions.withColumn(
        "line_amount",
        col("quantity_value") * col("price_value")
    )

def normalize_invoice_number(transactions):
    """Create a consistent invoice number while preserving the original."""

    # Remove surrounding spaces and standardize letters to uppercase.
    # Keep InvoiceNo unchanged so the original value remains traceable.
    return transactions.withColumn(
        "invoice_no",
        upper(trim(col("InvoiceNo")))
    )

def prepare_transaction_types(transactions):
    """Prepare invoice numbers, dates, quantities, and prices."""

    # Reuse the invoice normalization function we already tested.
    normalized = normalize_invoice_number(transactions)

    # Preserve the original columns and add typed versions.
    # TRY conversions return null for invalid values.
    # Later quality rules will route those rows to quarantine.
    return (
        normalized
        .withColumn(
            "invoice_timestamp",
            expr("try_to_timestamp(trim(InvoiceDate), 'M/d/yy H:mm')")
        )
        .withColumn(
            "quantity_value",
            expr("try_cast(trim(Quantity) AS INT)")
        )
        .withColumn(
            "price_value",
            expr("try_cast(trim(UnitPrice) AS DECIMAL(18,4))")
        )
    )


def add_rejection_reason(transactions):
    """Label invalid transactions without removing any rows."""

    # Apply this after prepare_transaction_types().
    # CASE records the first matching reason, keeping exclusions countable.
    return transactions.withColumn(
        "rejection_reason",
        expr("""
            CASE
                WHEN invoice_no IS NULL OR invoice_no = ''
                    THEN 'missing_invoice'
                WHEN invoice_timestamp IS NULL
                    THEN 'invalid_date'
                WHEN quantity_value IS NULL
                    THEN 'invalid_quantity'
                WHEN price_value IS NULL
                    THEN 'invalid_price'
                WHEN price_value <= 0
                    THEN 'non_positive_price'
                WHEN quantity_value = 0
                    THEN 'zero_quantity'
                WHEN quantity_value < 0 AND invoice_no NOT LIKE 'C%'
                    THEN 'negative_quantity_without_cancellation'
                WHEN quantity_value > 0 AND invoice_no LIKE 'C%'
                    THEN 'cancellation_with_positive_quantity'
                ELSE NULL
            END
        """)
    )