package avsc

const SchemaBlocks = `
{
  "name": "Block",
  "type": "record",
  "fields": [
    {
      "name": "timestamp",
      "type": "long",
      "logicaltype": "timestamp-micros"
    },
    {
      "name": "earn",
      "type": "long",
      "default": 0
    },
    {
      "name": "fees",
      "type": "string"
    },
    {
      "name": "frac",
      "type": "long",
      "default": 0
    },
    {
      "name": "gen",
      "type": "string"
    },
    {
      "name": "gh",
      "type": "string"
    },
    {
      "name": "prev",
      "type": "string",
      "default": ""
    },
    {
      "name": "proto",
      "type": "string"
    },
    {
      "name": "rate",
      "type": "long"
    },
    {
      "name": "rnd",
      "type": "long",
      "default": 0
    },
    {
      "name": "rwcalr",
      "type": "long"
    },
    {
      "name": "rwd",
      "type": ["null","string"],
      "default": null
    },
    {
      "name": "seed",
      "type": "string"
    },
    {
      "name": "tc",
      "type": ["null","long"],
      "default": null      
    },
    {
      "name": "ts",
      "type": "long"
    },
    {
      "name": "txn",
      "type": ["null","string"],
      "default": null
    },
    { 
      "name": "nextbefore",
      "type": ["null","long"],
      "default": null      
    },
    { 
      "name": "nextswitch",
      "type": ["null","long"],
      "default": null      
    },
    { 
      "name": "nextproto",
      "type": ["null","string"],
      "default": null      
    },
    { 
      "name": "nextyes",
      "type": ["null","long"],
      "default": null      
    },
    { 
      "name": "upgradeyes",
      "type": ["null","boolean"],
      "default": null      
    },
    { 
      "name": "upgradedelay",
      "type": ["null","long"],
      "default": null      
    },
    { 
      "name": "upgradeprop",
      "type": ["null","string"],
      "default": null      
    }
  ]
}`
