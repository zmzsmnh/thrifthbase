var thrift = require('thrift'),
    HBase = require('./gen-nodejs/HBase.js'),
    HBaseTypes = require('./gen-nodejs/hbase1_types.js'),
    Table = require('./table.js');

function Client(host, port, callback) {
    this.connect(host, port, callback);
}

Client.connect = function(host, port, callback) {
	if (this.isConnected) {
		if (host === this.host && port === this.port) {
			throw new Error("HBase already connected");
		} else {
			this.disconnect();
		}
	}

    host = typeof host !== 'undefined' ? host : localhost;
    port = typeof port !== 'undefined' ? port : 9090;
    var conn = thrift.createConnection(host, port, {
        transport: thrift.TFramedTransport,
        protocol: thrift.TBinaryProtocol
    });

    var client = this;
    this.host = host;
    this.port = port;
    this.isConnected = false;

    conn.on('connect', function() {
        client.isConnected = true;
        console.log('HBase connected!');
        client.hbaseClient = thrift.createClient(HBase, conn);
        if (callback) {
            callback(client);
        }
    });

    conn.on('error', function(err) {
        throw err;
    });
};

Client.table = function(tbname, callback) {
	return new Table(tbname, this.hbaseClient);
};

Client.tables = function(callback) {
    this.ensureConnection();
    this.hbaseClient.getTableNames(function(err, res) {
        console.log(res.toString().split(','));
    });
};

Client.ensureConnection = function() {
    console.log(this.isConnected);
    if (typeof this.isConnected === 'undefined') {
        this.connect();
        throw new Error("HBase not connected!");
    } else if (!this.isConnected) {
        throw new Error("HBase not connected!");
    }
};

Client.disconnect = function(callback) {
	this.hbaseClient.end();
	this.isConnected = false;
};

module.exports = Client;