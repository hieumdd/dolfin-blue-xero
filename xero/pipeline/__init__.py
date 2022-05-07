from xero.pipeline import bank_transactions, contacts, invoices, purchase_orders

pipelines = {
    i.name: i
    for i in [
        j.pipeline
        for j in [
            bank_transactions,
            contacts,
            invoices,
            purchase_orders,
        ]
    ]
}
