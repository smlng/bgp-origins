# mongodb queries

## number of prefixes per timestamp (sorted)
```
db.origins.aggregate([{$group: {_id: '$timestamp', prefixes: {$push: '$prefix'} } },{$project: { num_prefixes: {$size: '$prefixes'}}},{$sort: {_id: 1}}])
```
db.runCommand({aggregate: "origins", pipeline: [ {$group: {_id: '$timestamp', prefixes: {$push: '$prefix'} } }, {$project: {num_prefixes: {$size: '$prefixes'} } }, {$sort: {_id: 1} } ], allowDiskUse: true } )
```

## origins lifetime

* get prefix to origin AS association life times
```
db.origins_lt.aggregate([{$project: {_id:0, pfx:1, asn:1, begin: {$min: '$ttl'}, until: {$max: '$ttl'} } }, {$group: {_id: {pfx: '$pfx', asn: '$asn'}, ttls: {$push: {$subtract: ['$until', '$begin']}} }} ] )
```
* get avg, max, and min prefix to origin AS association live time
```
db.origins_lt.aggregate([{ $sort: { pfx: -1 } },{$project: {_id:0, pfx:1, asn:1, begin: {$min: '$ttl'}, until: {$max: '$ttl'} } }, {$group: {_id: {pfx: '$pfx', asn: '$asn'}, ttls: {$push: {$subtract: ['$until', '$begin']}} }},
{$project: {num_ttl: {$size: '$ttls'}, avg_ttl: {$avg: '$ttls'}, max_ttl: {$max: '$ttls'}, min_ttl: {$min: '$ttls'} } }, {$sort: {max_ttl: -1}} ] )
```
* get number of origins per prefix, sorted by number descending
```
db.origins_lt.aggregate([{$group: {_id: '$pfx', origins: {$addToSet: '$asn'}}},{$project: {num_origins: {$size: '$origins'}}},{$sort: {num_origins: -1}}])
```
* same as the first but as _runCommand_
```
db.runCommand({aggregate: "origins_lt", pipeline: [{$project: {_id:0, pfx:1, asn:1, begin: {$min: '$ttl'}, until: {$max: '$ttl'} } }, {$group: {_id: {pfx: '$pfx', asn: '$asn'}, ttls: {$push: {$subtract: ['$until', '$begin']}}
}} ], allowDiskUse: true} )
```


