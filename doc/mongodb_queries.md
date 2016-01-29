# mongodb queries

## number of prefixes per timestamp (sorted)
```
db.origins.aggregate([{$group: {_id: '$timestamp', prefixes: {$push: '$prefix'} } },{$project: { num_prefixes: {$size: '$prefixes'}}},{$sort: {_id: 1}}])
```
