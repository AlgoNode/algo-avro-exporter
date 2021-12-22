package avsc

const SchemaBlocks = `
{
  "name": "Block",
  "type": "record",
  "fields": [
    {
      "name": "timestamp",
      "type": [
        "null",
        {
          "type": "long",
          "logicaltype": "timestamp-millis"
        }
      ],
      "default": null
    },
    {
      "name": "earn",
      "type": [
        "null",
        "long"
      ],
      "default": null
    },
    {
      "name": "fees",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "frac",
      "type": [
        "null",
        "long"
      ],
      "default": null
    },
    {
      "name": "gen",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "gh",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "prev",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "proto",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "rate",
      "type": [
        "null",
        "long"
      ],
      "default": null
    },
    {
      "name": "rnd",
      "type": [
        "null",
        "long"
      ],
      "default": null
    },
    {
      "name": "rwcalr",
      "type": [
        "null",
        "long"
      ],
      "default": null
    },
    {
      "name": "rwd",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "seed",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "tc",
      "type": [
        "null",
        "long"
      ],
      "default": null
    },
    {
      "name": "ts",
      "type": [
        "null",
        "long"
      ],
      "default": null
    }
  ]
}`
