# DynamoDbExportCsv

  [![NPM Version][npm-image]][npm-url]
  [![NPM Downloads][downloads-image]][downloads-url]

A simple library / CLI tool for exporting a dynamodb table to a CSV file.  CSV file can be written to local file system
or streamed to S3.

## Features
  * Write to local file system
  * Stream to S3
  * DynamoDb parallel scans to utilize provisioned throughput

## Installation

### CLI tool
``` bash
  $ [sudo] npm install DynamoDBExportCSV -g
```

### Library
``` bash
  $ npm install DynamoDBExportCSV --save
```

## Usage

### CLI tool
``` bash
  $ ./bin/DynamoDBExportCSV --awsregion "us-west-2" --awsid "<id>" --awssecret "<secret>" --table "<mytable>" --columns "<columna,columnb,columnc>" --gzip
```

### Library
```js
var csvExport = require('DynamoDbExportCsv');
var exporter = new csvExport('<accessKey>', '<secretKey>', '<awsRegion>');

exporter.exportTable('<tableName>', ['columna','columnb'], 4, true, 250, null, null, function(err) {
  console.info('Done');
});

```

This will create a sub directory in the current working directory with the same name as the table. It will use a
parallel scan to create 4 files simultaneously and create a new file every 250MB. The csv files will be compressed with gzip.

Parallel Scans are useful to maximize usage of throughput provisioned on the DynamoDb table.

## Documentation

### new DynamoDbExportToCSV(awsAccessKeyId, awsSecretAccessKey, awsRegion)

Sets up the AWS credentials to use

__Arguments__

* `awsAccessKeyId` - AWS access key
* `awsSecretAccessKey` - AWS secret
* `awsRegion` - AWS region

### exportTable(table, columns, totalSegments, compressed, filesize, s3Bucket, s3Key, callback)

Exports the specified columns in the dynamodb table to one or more files

__Arguments__

* `table` - Name of dynamodb table
* `columns` - Array of column names.  Dynamodb has no way to query the table and determine columns without scanning
the entire table.  Only columns specified here are exported.  Columns do not have to be present on every
record.
* `totalSegments` - Number of parallel scans to run.  The dynamodb table key space is split into this many segments
and reads are done in parallel across these segments.  This spreads the query load across the key space and allows
higher read throughput. One file is created per segment at a minimum.
* `compressed` - When set to true files are output in compressed gzip format
* `filesize` - Maximum size of each file in megabytes.  Once file hits this size it is closed and a new file
created
* `s3Bucket` - Optional.  If specified the files are streamed to s3 instead of the local file system.
* `s3Key` - Optional. Key prefix for files in s3.  Used as a prefix with sequential numbers
appended for each file created
* `callback(err)` - A callback which is executed when finished and includes any errors that occurred

## People

The author is [Chris Kinsman](https://github.com/chriskinsman) from [PushSpring](http://www.pushspring.com)

## License

  [MIT](LICENSE)

[npm-image]: https://img.shields.io/npm/v/dynamodbexportcsv.svg?style=flat
[npm-url]: https://npmjs.org/package/dynamodbexportcsv
[downloads-image]: https://img.shields.io/npm/dm/dynamodbexportcsv.svg?style=flat
[downloads-url]: https://npmjs.org/package/dynamodbexportcsv
