var net           = require('net');
var msgpack       = require('msgpack'); 
var util          = require('util');

var PeerToPeer = exports.PeerToPeer = function(port, localNode) {
	this.port = port;
	this.localNode = localNode;
}

PeerToPeer.prototype.start = function(callback) {
	var self = this;
  	this.server = net.createServer(function (net_stream) {
    var mp_stream = new msgpack.Stream(net_stream);
    	mp_stream.on('msg', function(msg) { self.handleMessage(net_stream, mp_stream, msg) });
  	});
  	this.server.listen(this.port, callback);
}

PeerToPeer.prototype.stop = function() {
  this.server.close();
}

PeerToPeer.prototype.sendToPeer = function(peer, req_msg, callback, endCallback) {
	var a = peer.split(":");
	var peerConn = new net.createConnection(a[1], a[0]);
  	var self = this;
  	peerConn.on('connect', function(net_stream) {
    	var mp_stream = new msgpack.Stream(peerConn);
    	mp_stream.on('msg', function(msg) { self.handleMessage(peerConn, mp_stream, msg, callback) });
    	mp_stream.send(req_msg);
  	});
  	peerConn.on('error', function(exception) {
		callback(null, exception);
		if (endCallback) {
			endCallback(null, exception);			
		}
  	});
 	peerConn.on('end', function() {
		if (endCallback) {
			endCallback(undefined, undefined, 'end');			
		}
	});
}

PeerToPeer.GET_ENTRY_REQUEST = 0;
PeerToPeer.GET_ENTRY_RESPONSE = 1;
PeerToPeer.NEW_START_TOKEN_REQUEST = 2;
PeerToPeer.GET_DATA_REQUEST = 3;
PeerToPeer.GET_DATA_RESPONSE = 4;
PeerToPeer.ADD_ENTRY_REQUEST = 5;
PeerToPeer.REMOVE_ENTRY_REQUEST = 6;
PeerToPeer.QUERY_REQUEST = 7;
PeerToPeer.QUERY_RESPONSE = 8;


PeerToPeer.prototype.handleMessage = function(net_stream, mp_stream, msg, callback) {
	switch(msg.type) {
		
		//Collection MSG 
    	case PeerToPeer.GET_ENTRY_REQUEST:
			mp_stream.send({
				'type'  : PeerToPeer.GET_ENTRY_RESPONSE,
				'entry' : this.localNode.getOrCreateCollection(msg.collection).local.getEntry(msg.entryKey)
			});
			break;
    	case PeerToPeer.GET_ENTRY_RESPONSE:
			callback(msg.entry);
			break;
    	case PeerToPeer.ADD_ENTRY_REQUEST:
			this.localNode.getOrCreateCollection(msg.collection).local.addEntry(msg.entry);
			break;
    	case PeerToPeer.REMOVE_ENTRY_REQUEST:
			this.localNode.getOrCreateCollection(msg.collection).local.removeEntry(msg.key, msg.timestamp);
			break;

		//Node MSG 
	    case PeerToPeer.NEW_START_TOKEN_REQUEST:
			this.localNode.newStartToken(msg.newStartToken);
			break;		
		case PeerToPeer.GET_DATA_REQUEST:
			var colls = this.localNode.allCollections();
			var data = {};
			for (var i in colls) {
				data[colls[i].local.getName()] = colls[i].local.getData(msg.start, msg.end);
			}
			mp_stream.send({
				'type'  : PeerToPeer.GET_DATA_RESPONSE,
				'entries' : data
			});
			break;
    	case PeerToPeer.GET_DATA_RESPONSE:
			callback(msg.entries);
			break;

		//Query
		case PeerToPeer.QUERY_REQUEST:
			var colls = this.localNode.allCollections();
			var data = {};
			for (var i in colls) {
				data[colls[i].local.getName()] = colls[i].local.queryData(msg.query);
			}
			mp_stream.send({
				'type'  : PeerToPeer.QUERY_RESPONSE,
				'entries' : data
			});
			break;
    	case PeerToPeer.QUERY_RESPONSE:
			callback(msg.entries);
			break;
		
		default:
	      // shit went bad
	      break;
  	}
  	net_stream.end();
}

PeerToPeer.prototype.queryData = function(peer, query, callback) {
	this.sendToPeer(peer, {
	    'type'            : PeerToPeer.QUERY_REQUEST,
	    'query'           : query
	  }, callback);
	
}

PeerToPeer.prototype.getData = function(peer, start, end, callback) {
	this.sendToPeer(peer, {
	    'type'            : PeerToPeer.GET_DATA_REQUEST,
	    'start'           : start,
		'end'             : end
	  }, callback);
	
}

PeerToPeer.prototype.getEntry = function(peer, collection, key, callback) {
	this.sendToPeer(peer, {
	    'type'            : PeerToPeer.GET_ENTRY_REQUEST,
		'collection'      : collection,
	    'entryKey'        : key
	  }, callback);
	
}

PeerToPeer.prototype.addEntry = function(peer, collection, entry, callback) {
	this.sendToPeer(peer, {
	    'type'            : PeerToPeer.ADD_ENTRY_REQUEST,
		'collection'      : collection,
	    'entry'           : entry
	  }, function(){}, callback);
}

PeerToPeer.prototype.removeEntry = function(peer, collection, key, timestamp, callback) {
	this.sendToPeer(peer, {
	    'type'            : PeerToPeer.REMOVE_ENTRY_REQUEST,
	    'key'             : key,
		'collection'      : collection,
		'timestamp'       : timestamp
	}, function(){}, callback);
}


PeerToPeer.prototype.setNewStartToken = function(peer, newStartToken, callback) {
	this.sendToPeer(peer, {
	    'type'            : PeerToPeer.NEW_START_TOKEN_REQUEST,
	    'newStartToken'   : newStartToken
	  }, function(){}, callback);
}
