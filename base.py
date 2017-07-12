from functional import seq
from ledgertools import ledger

ledger_trans = ledger.get_transactions('/home/harterrt/Private/account_data/new.ledger')

print(
    seq(ledger_trans)\
        .map(lambda x: (x['category'], 1))\
        .reduce_by_key(lambda x, y: x + y)
        .filter(lambda x: 'Expenses' not in x[0])
        .sorted(key=lambda x: x[1])
)

exclude = [
    'CREDIT CARD',
    'Investor Checking',
    'COMPLETE ADVANTAGE(RM)',
    'SAVINGS',
]
grouped = (
    seq(ledger_trans)
        .filter(lambda x: x['category'] not in exclude)
        .map(lambda x: ((x['payee'], x['date']), x['category']))
        .group_by_key()
        .filter(lambda x: len(set(x[1])) > 1)
)

print(len(ledger_trans))/2
print(grouped.map(lambda x: len(x[1])).sum())
for x in grouped:
    print x
