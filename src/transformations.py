from pyspark.sql.functions import col, trim, upper


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