# Flows Field Syncer

A service to provide a way to synchronize flows contact fields with external data resources. Can be configured for mapping fields to columns or attributes
from external data to be fetched, each configuration will executed in defined time.

1. [Requirements](#requirements)
2. [Quickstart](#quickstart)
3. [Usage](#usage)

## Requirements

* Go 1.21 or higher
* MongoDB
* PostgreSQL

## Current Integrations Supported

* Athena
* BigQuery
* PostgreSQL

## Environment variables

Variable | Required | Default
-|-|-|
FLOWS_DB | false | postgres://temba:temba@localhost/temba?sslmode=disable
MONGO_URI | false | mongodb://localhost:27017
MONGO_DB_NAME | false | flows-field-syncer
MONGO_CONNECTION_TIMEOUT | false | 30 
HOST_API | false | :
PORT_API | false | 8080 
SENTRY_DSN | false |
LOG_LEVEL | false | debug 


## Quickstart

setup env variables

## Development

clone repository:

```bash
git clone https://github.com/weni-ai/flows-field-syncer.git
```

configure environment variables, and run:

```bash
go run cmd/main.go
```

## Usage

Manage sync configurations

### Create


Request:

`POST: /config`
```
{
  "service": {
    "name": STRING,
    "type": STRING,
    "access": OBJECT
  },
  "sync_rules": {
    "time": STRING,
    "interval": INT,
    "org_id": INT,
    "admin_id": INT
  },
  "table": {
      "name": STRING,
      "table_destination": STRING,
      "relation_column": STRING,
      "columns": [
        {
          "name": STRING,
          "field_map_name": STRING
        },
        ...
      ]
    }
}
```

### Read

Request:

`GET /config/<SYNCER_CONFIG_ID>`
```
no body
```


### Update

Request:

`PUT /config/<SYNCER_CONFIG_ID>`
```
{
  "service": {
    "name": STRING,
    "type": STRING,
    "access": OBJECT
  },
  "sync_rules": {
    "time": STRING,
    "interval": INT,
    "org_id": INT,
    "admin_id": INT
  },
  "table": {
      "name": STRING,
      "table_destination": STRING,
      "relation_column": STRING,
      "columns": [
        {
          "name": STRING,
          "field_map_name": STRING
        },
        ...
      ]
    }
}
```


### Delete

Request:

`DELETE /config/<SYNCER_CONFIG_ID>`

```
no body
```