from functional import seq
from ledgertools import ledger, mint, categorize

ledger_trans = ledger.get_transactions('/home/harterrt/Private/account_data/new.ledger')

# Let's not look at bank accounts
exclude = [
    'CREDIT CARD',
    'Investor Checking',
    'COMPLETE ADVANTAGE(RM)',
    'SAVINGS',
]

# Group categorizations by key (payee, date)
grouped = (
    seq(ledger_trans)
        .filter(lambda x: x['category'] not in exclude)
        .map(lambda x: ((x['payee'], x['date']), x['category']))
        .group_by_key()
)

# Problematic transactions have multiple categories for each key
problematic = grouped.filter(lambda x: len(set(x[1])) > 1)

# Look at the first transaction for each key
category_lookup = (
    grouped
        .map(lambda x: (x[0], x[1][0]))
        .to_dict()
)

# Read all mint transactions
all_trans = mint.get_data('/home/harterrt/Downloads/transactions.csv')
keyed_trans = (
    seq(all_trans)
        .map(lambda tran: (
            (tran['description'], tran['date']),
            tran
        ))
        .filter(lambda x: x[0] in category_lookup)
)
print("Total categorized new transactions: {0}".format(len(ledger_trans)/2))
print("Total matching new transactions: {0}".format(keyed_trans.len()))

keys = keyed_trans.map(lambda x: x[0])
print(seq(category_lookup.keys()).filter(lambda x: x not in keys))

with open('/home/harterrt/Private/account_data/fixed.ledger', 'w') as outfile:
    for (key, tran) in keyed_trans:
        outfile.write(categorize.to_ledger_format(tran, category_lookup[key]))

