var thrift = require('thrift'),
    HBase = require('./gen-nodejs/THBaseService'),
    HBaseTypes = require('./gen-nodejs/hbase2_types'),
    Table = require('./table'),
    poolModule = require('generic-pool');

function Client(host, port, callback) {
    this.create(host, port, callback);
}

Client.create = function(host, port, threads, callback) {
    if (this.isConnected) {
        if (host === this.host && port === this.port) {
            throw new Error("HBase already connected");
        } else {
            this.disconnect();
        }
    }

    this.host = typeof this.host !== 'undefined' ? this.host : 'localhost';
    this.port = typeof this.port !== 'undefined' ? this.port : 9090;
    host = typeof host !== 'undefined' ? host : this.host;
    port = typeof port !== 'undefined' ? port : this.port;

    var hbaseClient = this;
    this.host = host;
    this.port = port;
    this.isConnected = false;
    this.hbasePool = poolModule.Pool({
        name: 'hbase',
        create: function(callback) {
            var conn = thrift.createConnection(host, port, {
                transport: thrift.TFramedTransport,
                protocol: thrift.TBinaryProtocol
            });

            conn.on('connect', function() {
                console.log('HBase connected! ' + hbaseClient.hbasePool.getPoolSize() + ' connections are now available.');
                hbaseClient.isConnected = true;
                var client = thrift.createClient(HBase, this);
                client.connection = conn;
                callback(null, client);
            });

            conn.on('error', function(err) {
                throw err;
            });
        },
        destroy: function(client) {
            client.connection.end();
            console.log('HBase disconnected! ' + hbaseClient.hbasePool.getPoolSize() + ' connections left.');
        },
        max: threads,
        min: 2,
        idleTimeoutMillis: 6000,
        refreshIdle: false,
        log: false
    });

    callback(this);
};

Client.table = function(tbname, callback) {
    return new Table(tbname, this.hbasePool);
};

// Client.tables = function(callback) {
//     // this.ensureConnection();
//     var pool = this.hbasePool;
//     pool.acquire(function(err, client) {
//         client.getTableNames(function(err, res) {
//             console.log(res.toString().split(','));
//             pool.release(client);
//             callback(res);
//         });
//     });
// };

Client.ensureConnection = function() {
    console.log(this.isConnected);
    if (typeof this.isConnected === 'undefined') {
        this.create();
        throw new Error("HBase not connected!");
    } else if (!this.isConnected) {
        throw new Error("HBase not connected!");
    }
};

Client.disconnect = function(callback) {
    this.hbasePool.destroyAllNow();
    this.isConnected = false;
};

module.exports = Client;