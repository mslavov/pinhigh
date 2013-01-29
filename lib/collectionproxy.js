var bignum             = require('bignum');
var Query              = require('./query.js').Query;
var QueryResultHandler = require('./queryresulthandler.js').QueryResultHandler;
var Logger             = require('./logger.js').Logger;
var NodeConst          = require('./const.js').NodeConst;
var collectionUtil     = require('./collectionutil');
var util               = require('util');

var CollectionProxy = exports.CollectionProxy = function(nodeName, name, replicas, p2p, gossiper) {
	this.name = name;
	this.replicas = replicas;
	this.logger = new Logger("CollectionProxy[" + nodeName + "][" + this.name + "]");
	this.p2p = p2p;
	this.gossiper = gossiper;
	this.cached = {};
}

CollectionProxy.prototype.getName = function() {
	return this.name;
}

//REMOTE API
CollectionProxy.prototype.query = function(query, callback) {
	var collQuery = {
		'and' : {
			'collection' : this.name
		}
	}
	for (var i in query) {
		collQuery.and[i] = query[i];
	}
	var peers = this.gossiper.allPeers();
	this.logger.debug("Quering peers: " + peers);
	var queryResultHandler = new QueryResultHandler(callback);
	for(var i in peers) {
		this.p2p.queryData(this.getDataPort(peers[i]), collQuery, queryResultHandler.queryHandler());		
	}
}

CollectionProxy.prototype.put = function(key, value, callback) {
	var entry = collectionUtil.createEntry(key, value);
	this.logger.debug("API: put; Entry: " + entry);
	var peers = this.getPeers(entry.hash);
	this.logger.debug("API: put; Peers: " + peers);
	if (peers.length == 0) {
		callback(null, "No peers found");
		return;
	}
	var self = this;
	var replicaCallback = function(data, error, end) {
		if (end) {
			self.logger.debug("Storing replica end");
		}
		if (error) {
			self.logger.warn("Error storing replica: " + error);
		}
	}
	for (var i in peers) {
		this.logger.debug("API: put; Sending entry to peer: " + peers[i]);
		//Replica handler
		this.p2p.addEntry(this.getDataPort(peers[i]), this.name, entry, ( i == 0 ? callback : replicaCallback));
	}
}

CollectionProxy.prototype.get = function(key, callback) {
	var hash = collectionUtil.getHashForKey(key);
	this.logger.debug("API: get; Key: " + key + ", hash: " + hash);
	var peers = this.getPeers(hash);
	this.logger.debug("API: get; Peers: " + peers);
	if (peers.length == 0) {
		callback(null, "No peers found");
		return;
	}
	this.logger.debug("API: get; try with primary: " + peers[0]);
	this.p2p.getEntry(this.getDataPort(peers[0]), this.name, key, this.getEntryHandler(peers, 0, key, callback));
}

CollectionProxy.prototype.remove = function(key, callback) {
	var hash = collectionUtil.getHashForKey(key);
	this.logger.debug("API: remove; Key: " + key + ", hash: " + hash);
	var peers = this.getPeers(hash);
	this.logger.debug("API: remove; Peers: " + peers);
	if (peers.length == 0) {
		callback(null, "No peers found");
		return;
	}
	var self = this;
	var replicaCallback = function(data, error, end) {
		if (end) {
			self.logger.debug("Removing replica end");
		}
		if (error) {
			self.logger.warn("Error removing replica: " + error);
		}
	}
	for (var i in peers) {
		this.logger.debug("API: put; Sending entry to peer: " + peers[i]);
		this.p2p.removeEntry(this.getDataPort(peers[i]), this.name, key, new Date().getTime(),  ( i == 0 ? callback : replicaCallback));
	}	
}

//HELPER API
CollectionProxy.prototype.getEntryHandler = function(peers, index, key, callback) {
	var self = this;
	return function(data, error, end) {
		if (end) {
			return;
		}
		self.logger.debug("API: get; GetEntryHandler: " + data + " error: " + error);
		if (error) {
			var newIndex = index + 1;
			if (newIndex < peers.length) {
				self.logger.debug("API: get; GetEntryHandler: try peer at: " + newIndex);
				self.p2p.getEntry(self.getDataPort(peers[newIndex]), self.name, key, self.getEntryHandler(peers, newIndex, key, callback));
			} else {
				self.logger.debug("API: get; GetEntryHandler: no more peers to try");
				callback(null, error);
			}
			return;
		}
		if (data) {
			callback(data.value);			
		} else {
			callback(undefined);
		}
	}
}

CollectionProxy.prototype.getDataPort = function(peer) {
	return this.gossiper.peerValue(peer, NodeConst.DATA_PORT);	
}

CollectionProxy.prototype.contains = function(p1, p2) {
	var p1S = bignum(this.gossiper.peerValue(p1, NodeConst.TOKEN_START));
	var p1E = bignum(this.gossiper.peerValue(p1, NodeConst.TOKEN_END));
	var p2E = bignum(this.gossiper.peerValue(p2, NodeConst.TOKEN_END));
	return collectionUtil.valInRange(p2E, p1S, p1E);
}

CollectionProxy.prototype.getOverlapping = function(peer) {
	var peers = this.gossiper.allPeers();
	this.logger.debug("Checking for overlapping peers: " + peers);
	var primaryPeer = null;
	var secondaryPeer = null;
	for(var i in peers) {
		if (this.gossiper.peerValue(peers[i], NodeConst.TOKEN_END)) {
			if (this.contains(peer, peers[i])) {
				this.logger.debug("Overlap detectred");
				primaryPeer = peers[i];
				secondaryPeer = peer;
			} else if (this.contains(peers[i], peer)) {
				this.logger.debug("Overlap detectred");
				primaryPeer = peer;
				secondaryPeer = peers[i];
			}
		}
	}
	if (primaryPeer == null) {
		primaryPeer = peer;
	}
	this.logger.debug("Overlap - primary: " + primaryPeer);
	this.logger.debug("Overlap - secondary: " + secondaryPeer);
	return {
		'primary' : primaryPeer,
		'secondary' : secondaryPeer
	};	
}


CollectionProxy.prototype.getPeers = function(hash) {
	this.logger.debug("Trying to find primary node for hash: " + hash);
	var peers = this.gossiper.allPeers();
	this.logger.debug("Checking peers: " + peers);
	var primaryPeer = null;
	var secondaryPeer = null;
	var replicaPeers = [];
	for(var i in peers) {
		if (this.gossiper.peerValue(peers[i], NodeConst.TOKEN_END)) {
			var peerTokenStart = bignum(this.gossiper.peerValue(peers[i], NodeConst.TOKEN_START));
			var peerTokenEnd = bignum(this.gossiper.peerValue(peers[i], NodeConst.TOKEN_END));
			this.logger.debug("Checking peer [" + peerTokenStart + ".." + peerTokenEnd + "]");
			if (collectionUtil.valInRange(hash, peerTokenStart, peerTokenEnd)) {
				this.logger.debug("Primary node found");
				//Check overlapping.
				var overlapping = this.getOverlapping(peers[i]);
				primaryPeer = overlapping.primary;
				secondaryPeer = overlapping.secondary;
				break;
			}
		}
	}
	if (primaryPeer != null) {
		var nextPeer = primaryPeer;
		replicaPeers.push(nextPeer);
		if (secondaryPeer != null) {
			nextPeer = secondaryPeer;
			replicaPeers.push(nextPeer);
		}
		for (var i = 0; i < this.replicas; i++) {
			nextPeer = this.getNextPeer(primaryPeer, nextPeer);
			if (nextPeer == null) {
				this.logger.debug("No more next peers, abort search and continue...");
				break;
			}
			replicaPeers.push(nextPeer);
		}
	}
	return replicaPeers;
}

CollectionProxy.prototype.getNextPeer = function(start, peer) {
	var tokenEnd = bignum(this.gossiper.peerValue(peer, NodeConst.TOKEN_END));
	this.logger.debug("Looking for next peer starting at: " + tokenEnd);
	var peers = this.gossiper.allPeers();
	var nextPeer = null;
	for(var i in peers) {
		if (this.gossiper.peerValue(peers[i], NodeConst.APP_STATE) == "ready") {
			var peerTokenStart = bignum(this.gossiper.peerValue(peers[i], NodeConst.TOKEN_START));
			this.logger.debug("Checking node starting at: " + peerTokenStart);
			if (tokenEnd.eq(peerTokenStart)) {
				this.logger.debug("Next node found");
				nextPeer = peers[i];
				break;
			}
		}
	}
	//if we reached the start return null
	if (nextPeer != null && this.gossiper.peerValue(start, NodeConst.NODE_NAME) == this.gossiper.peerValue(nextPeer, NodeConst.NODE_NAME)) {
		this.logger.debug("Next node eq the primary");
		nextPeer = null;
	}
	return nextPeer;
}