package avsc

const SchemaTXNs = `
  {
    "type": "record",
    "name": "TXN",
    "fields": [
      {
        "name": "timestamp",
        "type": "long",
         "logicalType": "timestamp-micros"
      },
      {
        "name": "rnd",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "id",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "type",
        "type": [
          "null",
          "string"
        ],"default": null        
      },
      {
        "name": "sig",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "note",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "amt",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "fee",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "fv",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "lv",
        "type": [
          "null",
          "long"
        ],"default": null        

      },
      {
        "name": "rcv",
        "type": [
          "null",
          "string"
        ],"default": null        
      },
      {
        "name": "snd",
        "type": [
          "null",
          "string"
        ],"default": null        
      },
      {
        "name": "aamt",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "arcv",
        "type": [
          "null",
          "string"
        ],"default": null        
      },
      {
        "name": "asnd",
        "type": [
          "null",
          "string"
        ],"default": null        
      },
      {
        "name": "xaid",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "afrz",
        "type": [
          "null",
          "boolean"
        ],"default": null        
      },
      {
        "name": "fadd",
        "type": [
          "null",
          "string"
        ],"default": null        
      },
      {
        "name": "faid",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "apap",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "apgs",
        "type": [
          "null",
          {
            "type": "record",
            "namespace": "root",
            "name": "Apgs",
            "fields": [
              {
                "name": "nbs",
                "type": [
                  "null",
                  "long"
                ],"default": null        
              },
              {
                "name": "nui",
                "type": [
                  "null",
                  "long"
                ],"default": null        
              }
            ],"default": null        
          }
        ],"default": null        
      },
      {
        "name": "apsu",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "apan",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "apid",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "apaa",
        "type": 
          ["null", {"type": "array","items": "bytes"}]
        , "default" : null
      },
      {
        "name": "apat",
        "type": 
          ["null", {"type": "array","items": "string"}]
        , "default" : null
      },
      {
        "name": "apfa",
        "type": 
          ["null", { "type": "array",  "items": "long"}]
        , "default": null
      },
      {
        "name": "apas",
        "type": 
          ["null", { "type": "array",  "items": "long"}]
        , "default": null
      },
      {
        "name": "apls",
        "type": [
          "null",
          {
            "type": "record",
            "namespace": "root",
            "name": "Apls",
            "fields": [
              {
                "name": "nbs",
                "type": [
                  "null",
                  "long"
                ],"default": null        
              },
              {
                "name": "nui",
                "type": [
                  "null",
                  "long"
                ],"default": null        
              }
            ],"default": null        
          }
        ],"default": null        
      },
      {
        "name": "apep",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "selkey",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "votefst",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "votekd",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "votekey",
        "type": [
          "null",
          "bytes"
        ],"default": null        
      },
      {
        "name": "votelst",
        "type": [
          "null",
          "long"
        ],"default": null        
      },
      {
        "name": "close",
        "type": [
          "null",
          "string"
        ],"default": null        
      }
    ],"default": null        
  }`
