from xero.pipeline import (
    accounts,
    assets,
    bank_transactions,
    contacts,
    invoices,
    purchase_orders,
    credit_notes,
)

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            accounts,
            bank_transactions,
            contacts,
            invoices,
            purchase_orders,
            credit_notes,
        ]
    ]
    + [
        assets.pipeline(status)
        for status in [
            "DRAFT",
            "REGISTERED",
            "DISPOSED",
        ]
    ]
}
