from xero.pipeline.interface import Pipeline
from xero.pipeline.utils import parse_timestamp

pipeline = Pipeline(
    "Contacts",
    "Contacts",
    lambda x: x["Contacts"],
    lambda rows: [
        {
            "ContactID": row.get("ContactID"),
            "ContactStatus": row.get("ContactStatus"),
            "Name": row.get("Name"),
            "FirstName": row.get("FirstName"),
            "LastName": row.get("LastName"),
            "CompanyNumber": row.get("CompanyNumber"),
            "EmailAddress": row.get("EmailAddress"),
            "SkypeUserName": row.get("SkypeUserName"),
            "BankAccountDetails": row.get("BankAccountDetails"),
            "TaxNumber": row.get("TaxNumber"),
            "AccountsReceivableTaxType": row.get("AccountsReceivableTaxType"),
            "AccountsPayableTaxType": row.get("AccountsPayableTaxType"),
            "Addresses": [
                {
                    "AddressType": address.get("AddressType"),
                    "AddressLine1": address.get("AddressLine1"),
                    "City": address.get("City"),
                    "PostalCode": address.get("PostalCode"),
                    "AttentionTo": address.get("AttentionTo"),
                }
                for address in row["Addresses"]
            ]
            if row.get("Addresses")
            else [],
            "Phones": [
                {
                    "PhoneType": phone.get("PhoneType"),
                    "PhoneNumber": phone.get("PhoneNumber"),
                    "PhoneAreaCode": phone.get("PhoneAreaCode"),
                    "PhoneCountryCode": phone.get("PhoneCountryCode"),
                }
                for phone in row["Phones"]
            ]
            if row.get("Phones")
            else [],
            "UpdatedDateUTC": parse_timestamp(row.get("UpdatedDateUTC")),
            "IsSupplier": row.get("IsSupplier"),
            "IsCustomer": row.get("IsCustomer"),
            "DefaultCurrency": row.get("DefaultCurrency"),
        }
        for row in rows
    ],
    [
        {"name": "ContactID", "type": "STRING"},
        {"name": "ContactStatus", "type": "STRING"},
        {"name": "Name", "type": "STRING"},
        {"name": "FirstName", "type": "STRING"},
        {"name": "LastName", "type": "STRING"},
        {"name": "CompanyNumber", "type": "STRING"},
        {"name": "EmailAddress", "type": "STRING"},
        {"name": "SkypeUserName", "type": "STRING"},
        {"name": "BankAccountDetails", "type": "STRING"},
        {"name": "TaxNumber", "type": "STRING"},
        {"name": "AccountsReceivableTaxType", "type": "STRING"},
        {"name": "AccountsPayableTaxType", "type": "STRING"},
        {
            "name": "Addresses",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "AddressType", "type": "STRING"},
                {"name": "AddressLine1", "type": "STRING"},
                {"name": "City", "type": "STRING"},
                {"name": "PostalCode", "type": "STRING"},
                {"name": "AttentionTo", "type": "STRING"},
            ],
        },
        {
            "name": "Phones",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "PhoneType", "type": "STRING"},
                {"name": "PhoneNumber", "type": "STRING"},
                {"name": "PhoneAreaCode", "type": "STRING"},
                {"name": "PhoneCountryCode", "type": "STRING"},
            ],
        },
        {"name": "UpdatedDateUTC", "type": "TIMESTAMP"},
        {"name": "IsSupplier", "type": "BOOLEAN"},
        {"name": "IsCustomer", "type": "BOOLEAN"},
        {"name": "DefaultCurrency", "type": "STRING"},
    ],
    "ContactID",
)
