# Accounts payable policy

Northwind Goods pays vendor invoices only after they match what we ordered and what we received. This policy applies to every invoice, whatever the amount.

## Three-way match

Every invoice is checked against three records before it is paid:

1. The purchase order (PO) the invoice cites: what we agreed to buy, and at what price.
2. The goods receipt for that PO: what the warehouse counted in.
3. The vendor record: the vendor on the PO is the vendor on the invoice, and the vendor is approved.

The invoice passes when all of these hold:

- The invoice names a PO that exists and is open.
- The vendor on the invoice is the vendor on the PO, and the vendor is approved in the vendor master.
- For each invoiced line, the quantity is no more than the quantity received. We do not pay for goods that have not arrived.
- For each invoiced line, the unit price is within 2% of the PO price, above or below.
- The invoice total is within $5.00 of the lines (quantity times unit price) plus tax. Larger differences are usually misread or altered figures.
- No other open invoice has the same vendor and invoice number.

An invoice that passes is "matched". An invoice that fails any rule is an exception.

## Exceptions and separation of duties

An AP clerk records and matches invoices. A clerk cannot approve an exception. An invoice with an exception goes to the controller, who chooses one of:

- Approve the exception and pay the invoiced amount, with a note saying why.
- Pay only the matched amount: each line at the lower of the invoiced and received quantity and the lower of the invoiced and PO price, with tax in proportion. The vendor is told the rest is disputed.
- Reject the invoice. The vendor must send a corrected invoice or a credit note.
- Put the invoice on hold while we ask the vendor or the warehouse a question.

The person who matches an invoice does not approve its exception, and the controller's decision and note stay on the invoice for audit.

## Payment approval

Scheduling a payment moves money, so a person approves every payment. The payment amount is the approved amount: the invoice total for a clean invoice, or the amount the controller chose. The clerk may approve the payment of a matched invoice. Only the controller approves the payment of an invoice that had exceptions.

## Payment terms

The pay date is the invoice date plus the vendor's payment terms. Net 30 means 30 days after the invoice date; Net 15 means 15 days. We pay on the pay date, not before, unless the vendor offers an early-payment discount that the controller has approved. A late invoice is paid on the next payment run.

## Duplicate prevention

Before an invoice is recorded, the system checks the vendor and invoice number. The same vendor, invoice number and total twice is refused at upload. The same number with a different total is recorded, but the match fails it as a possible duplicate and the controller decides. A rejected invoice does not count as a duplicate, so a vendor may resubmit a corrected one under the same number.

## Foreign currency and credit notes

Invoices are matched in the PO's currency. A credit note is matched against the invoice it corrects and reduces the amount paid; it is not paid as a separate invoice.
