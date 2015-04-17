var utils = require('./utils.js');
var thrift = require('thrift'),
    HBaseTypes = require('./gen-nodejs/hbase2_types');

function Table(name, hbasePool) {
    this.name = name;
    this.hbasePool = hbasePool;
    return this;
}

// Table.prototype.rows = function(rows, params, callback) {
//     var pool = this.hbasePool;
//     var tableName = this.name;
//     pool.acquire(function(err, client) {
//         if (params.columns) {
//             if (params.timestamp) {
//                 client.getRowsWithColumnsTs(tableName, rows, params.columns, params.timestamp, undefined, function(err, data) {
//                     pool.release(client);
//                     callback(data);
//                 });
//             } else {
//                 client.getRowsWithColumns(tableName, rows, params.columns, undefined, function(err, data) {
//                     pool.release(client);
//                     callback(data);
//                 });
//             }

//         } else {
//             if (params.timestamp) {
//                 client.getRowsTs(tableName, rows, params.timestamp, undefined, function(err, data) {
//                     pool.release(client);
//                     callback(data);
//                 });
//             } else {
//                 client.getRows(tableName, rows, undefined, function(err, data) {
//                     pool.release(client);
//                     callback(data);
//                 });
//             }
//         }
//     });
// };

Table.prototype.rows = function(rows, params, callback) {
    var pool = this.hbasePool;
    var tableName = this.name;
    var tGets = [];
    for (var i = 0; i < rows.length; i++) {
        var args = utils.cloneObj(params);
        args.row = rows[i];
        if (args.columns !== undefined) {
            var columns = [];
            for (var j = 0; j < args.columns.length; j++) {
                var arr = args.columns[j].split(':');
                columns.push(new HBaseTypes.TColumn({
                    family: arr[0],
                    qualifier: arr[1]
                }));
            }
            args.columns = columns;
        }
        // console.log(args);
        if (args.timeRange !== undefined) {
            args.timeRange = new HBaseTypes.TTimeRange(args.timeRange);
        }
        tGets.push(new HBaseTypes.TGet(args));
    }
    pool.acquire(function(err, client) {
        client.getMultiple(tableName, tGets, function(err, data) {
            pool.release(client);
            data = data.filter(function(doc) {
                return doc.row != null;
            });
            if (err) {
                throw err;
            } else {
                callback(data);
            }
        });
    });
};

module.exports = Table;