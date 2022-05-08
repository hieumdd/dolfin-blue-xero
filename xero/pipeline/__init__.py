from xero.pipeline import bank_transactions, contacts, invoices, purchase_orders, credit_notes

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            bank_transactions,
            contacts,
            invoices,
            purchase_orders,
            credit_notes,
        ]
    ]
}
