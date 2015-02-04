var utils = require('./utils.js');
var thrift = require('thrift');

function Table(name, hbase) {
    this.name = name;
    this.hbase = hbase;
    return this;
}

Table.prototype.rows = function(rows, params, callback) {
    if (params.columns) {
        if (params.timestamp) {
            this.hbase.getRowsWithColumnsTs(this.name, rows, params.columns, params.timestamp, undefined, function(err, data) {
                callback(data);
            });
        } else {
            this.hbase.getRowsWithColumns(this.name, rows, params.columns, undefined, function(err, data) {
                callback(data);
            });
        }

    } else {
        if (params.timestamp) {
            this.hbase.getRowsTs(this.name, rows, params.timestamp, undefined, function(err, data) {
                callback(data);
            });
        } else {
            this.hbase.getRows(this.name, rows, undefined, function(err, data) {
                callback(data);
            });
        }
    }
};

module.exports = Table;