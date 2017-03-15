'use strict';

var async = require('async');
var AWS = require('aws-sdk');
var csv = require('fast-csv');
var fs = require('fs');
var util = require('util');
var zlib = require('zlib');
var path = require('path');
var child_process = require('child_process');
var _ = require('underscore');
var s3StreamUpload = require('s3-stream-upload');

var EventEmitter = require('events').EventEmitter;

function DynamoDBExportCSV(awsAccessKeyId, awsSecretAccessKey, awsRegion) {
    // Events this emits
    var infoEvent = 'info';
    var errorEvent = 'error';
    var throughputExceededEvent = 'throughputexceeded';
    var _awsAccessKeyId = awsAccessKeyId;
    var _awsSecretAccessKey = awsSecretAccessKey;
    var _awsRegion = awsRegion;
    var _args = [];

    // Save off reference to this for later
    var self = this;

    // Call super constructor
    EventEmitter.call(this);

    // Configure dynamoDb
    var config = { region: _awsRegion };
    _args.push('--awsregion');
    _args.push(_awsRegion);
    if (_awsAccessKeyId) {
        config.accessKeyId = _awsAccessKeyId;
        _args.push('--awsid');
        _args.push(_awsAccessKeyId);
    }

    if (_awsSecretAccessKey) {
        config.secretAccessKey = _awsSecretAccessKey;
        _args.push('--awssecret');
        _args.push(_awsSecretAccessKey);
    }
    AWS.config.update(config);

    var dynamodb = new AWS.DynamoDB({ maxRetries: 20 });
    var s3 = new AWS.S3();

    // Total row count
    var rowCount = 0;

    // Writes out an item without regard for headers
    function writeItemWithoutHeaders(stream, item, callback) {
        var row = {};
        _.each(item, function (value, key) {
            row[key] = value.S || value.N || value.BOOL;
        });

        return stream.write(row, callback);
    }

    // Writes out an item and ensures that every specified column
    // is represented
    function writeItemWithHeaders(stream, item, columns, callback) {
        var row = {};
        _.each(columns, function (column) {
            if (item[column]) {
                row[column] = item[column].S || item[column].N || item[column].BOOL;
            }
            else {
                row[column] = '';
            }
        });

        return stream.write(row, callback);
    }

    // Does the real work of writing the table to a CSV file
    function writeTableToCsv(table, columns, totalSegments, segment, compressed, filesize, s3Bucket, s3Path, callback) {
        var query = {
            TableName: table,
            Segment: segment,
            TotalSegments: totalSegments
        };

        var csvStream;
        var backoff = 1;
        // Count of files used to increment number in filename for each file
        var fileCount = 0;
        async.doWhilst(
            function (done) {
                // Form the filename with the table name as the subdirectory and the base of the filename
                // then th segemnt and the file within the segment
                var fileName = table + "-" + segment + "-" + fileCount + ".csv";
                if (compressed) {
                    fileName += ".gz";
                }

                csvStream = csv.createWriteStream({ headers: true, maxBufferSize: 10000 });

                var writableStream;
                if (s3Bucket) {
                    var filePath = '';
                    if (s3Path) {
                        filePath += s3Path + "/";
                    }
                    filePath += table + "/" + fileName;
                    writableStream = s3StreamUpload(s3, { Bucket: s3Bucket, Key: filePath }, { concurrent: totalSegments });
                    self.emit(infoEvent, "Starting new file: s3://" + s3Bucket + "/" + filePath);
                }
                else {
                    writableStream = fs.createWriteStream(table + '/' + fileName);
                    self.emit(infoEvent, "Starting new file: " + fileName);
                }

                // If we are compressing pipe it through gzip
                if (compressed) {
                    csvStream.pipe(zlib.createGzip()).pipe(writableStream);
                }
                else {
                    csvStream.pipe(writableStream);
                }

                var fileRowCount = 0;

                // drain check function


                // Repeatedly scan dynamodb until there are no more rows
                async.doWhilst(
                    function (done) {
                        var noDrainRequired = false;
                        dynamodb.scan(query, function (err, data) {
                            if (err) {
                                // Check for throughput exceeded
                                if (err.code && err.code == "ProvisionedThroughputExceededException") {
                                    self.emit(throughputExceededEvent);
                                    // Wait at least one second before the next query
                                    setTimeout(function () { return done(null); }, backoff * 1000);
                                    // Increment backoff
                                    backoff *= 2;
                                }
                                else {
                                    return setImmediate(function () { done(err); });
                                }
                            }
                            else {
                                // Reset backoff
                                backoff = 1;
                                if (data) {
                                    // Grab the key to start the next scan on
                                    query.ExclusiveStartKey = data.LastEvaluatedKey;

                                    async.eachSeries(data.Items, function (item, done) {
                                        async.series([
                                            function (done) {
                                                if (fileRowCount === 0) {
                                                    noDrainRequired = writeItemWithHeaders(csvStream, item, columns, done);
                                                }
                                                else {
                                                    noDrainRequired = writeItemWithoutHeaders(csvStream, item, done);
                                                }
                                            }
                                        ], function (err) {
                                            if (err) {
                                                self.emit(errorEvent, err);
                                            }
                                            fileRowCount++;
                                            rowCount++;

                                            // Check if we need to drain to avoid bloating memory
                                            if (!noDrainRequired) {
                                                csvStream.once('drain', function () {
                                                    return setImmediate(function () { done(null); });
                                                })
                                            }
                                            else {
                                                return setImmediate(function () { done(null); });
                                            }
                                        });

                                    }, function (err) {
                                        return setImmediate(function () { done(null); });
                                    });
                                }
                                else {
                                    return setImmediate(function () { done(null); });
                                }
                            }
                        });
                    },
                    function () {
                        self.emit(infoEvent, "Row: " + rowCount + ", Mb: " + (writableStream.bytesWritten / 1024 / 1024).toFixed(2));
                        // Keep going if there is more data and we haven't exceeded the file size
                        return query.ExclusiveStartKey && writableStream.bytesWritten < 1024 * 1024 * filesize;
                    },
                    function (err) {
                        if (err) {
                            return setImmediate(function () { done(err); });
                        }
                        else {
                            // End the stream
                            if (csvStream) {
                                csvStream.end();
                            }
                            fileCount++;
                        }
                    }
                );

                // Wait for the stream to emit finish before we return
                // When gzipped this can take a bit
                writableStream.on('finish', function () {
                    self.emit(infoEvent, "Finished file: " + fileName);
                    return setImmediate(function () { done(null); });
                });
            },
            function () {
                // Keep going if we still have more data
                return query.ExclusiveStartKey;
            },
            callback
        );
    }

    // Public export table function
    this.exportTable = function exportTable(table, columns, totalSegments, compressed, filesize, s3Bucket, s3Path, callback) {
        if (!filesize) {
            filesize = 250;
        }

        async.series([
            // Create a directory based on the table name if one doesn't exist
            function (done) {
                // Only if we aren't uploading to s3
                if (!s3Bucket) {
                    fs.exists(table, function (exists) {
                        if (!exists) {
                            fs.mkdir(table, done);
                        }
                        else {
                            return setImmediate(done);
                        }
                    });
                }
                else {
                    return setImmediate(done);
                }
            },
            // Scan the table
            function (done) {
                var parallelScanFunctions = [];
                var cli = path.join(__dirname, '../bin/DynamoDbExportCsv');
                var allWorkerArgs = _.union(_args, ['--table', table, '--columns', columns, '--scans', totalSegments]);
                if (compressed) {
                    allWorkerArgs.push('--gzip');
                }
                if (filesize) {
                    allWorkerArgs.push('--filesize');
                    allWorkerArgs.push(filesize);
                }
                if (s3Bucket) {
                    allWorkerArgs.push('--bucket');
                    allWorkerArgs.push(s3Bucket);
                }
                if (s3Path) {
                    allWorkerArgs.push('--path');
                    allWorkerArgs.push(s3Path);
                }
                for (var i = 0; i < totalSegments; i++) {
                    parallelScanFunctions.push(
                        function (worker) {
                            return function (done) {
                                var args = _.union(allWorkerArgs, ['--worker', worker]);
                                var child = child_process.spawn(cli, args, {});
                                var logPrefix = 'Worker [' + worker + ']: ';
                                child.stdout.on('data', function (data) { self.emit(infoEvent, logPrefix + data.slice(0, -1)); });
                                child.stderr.on('data', function (data) { self.emit(errorEvent, logPrefix + data.slice(0, -1)); });
                                child.on('error', function (err) { self.emit(errorEvent, logPrefix + err); });
                                child.on('exit', function (code) { self.emit(infoEvent, logPrefix + 'Exit Code ' + code); done(); });
                            };
                        }(i)

                        // function(segment) {
                        //     return function(done) {
                        //         writeTableToCsv(table, columns, totalSegments, segment, compressed, filesize, s3Bucket, s3Path, done);
                        //     };
                        // }(i)
                    );
                }

                async.parallel(parallelScanFunctions, done);
            }
        ], callback);
    };

    // Public export table function
    this.exportTableWorker = function exportTableWorker(table, columns, totalSegments, segment, compressed, filesize, s3Bucket, s3Path, callback) {
        if (!filesize) {
            filesize = 250;
        }

        async.series([
            // Create a directory based on the table name if one doesn't exist
            function (done) {
                // Only if we aren't uploading to s3
                if (!s3Bucket) {
                    fs.exists(table, function (exists) {
                        if (!exists) {
                            fs.mkdir(table, done);
                        }
                        else {
                            return setImmediate(done);
                        }
                    });
                }
                else {
                    return setImmediate(done);
                }
            },
            // Scan the table
            function (done) {
                writeTableToCsv(table, columns, totalSegments, segment, compressed, filesize, s3Bucket, s3Path, done);
            }
        ], callback);
    };


    return (this);
}

util.inherits(DynamoDBExportCSV, EventEmitter);

module.exports = DynamoDBExportCSV;