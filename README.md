# datax-kudu-plugins
datax kudu的writer插件



eg:

```json
{
  "name": "kudu11xwriter",
  "parameter": {
    "kuduConfig": {
      "kudu.master_addresses": "***"
    },
    "table": "",
    "numReplicas": 3,
    "truncate": false,
    "insertMode": "upsert",
    "partition": {
      "range": {
        "column1": [
          {
            "lower": "2020-08-25",
            "upper": "2020-08-26"
          },
          {
            "lower": "2020-08-26",
            "upper": "2020-08-27"
          },
          {
            "lower": "2020-08-27",
            "upper": "2020-08-28"
          }
        ]
      },
      "hash": {
        "column": [
          "column1"
        ],
        "num": 3
      }
    },
    "column": [
      {
        "index": 1,
        "name": "c1",
        "type": "string",
        "primaryKey": true
      },
      {
        "index": 2,
        "name": "c2",
        "type": "string",
        "compression": "DEFAULT_COMPRESSION",
        "encoding": "AUTO_ENCODING",
        "comment": "注解xxxx"
      }
    ],
    "writeBufferSize": 1024,
    "mutationBufferSpace": 2048,
    "encoding": "UTF-8"
  }
}
```

bug、问题交流请联系QQ:912456357