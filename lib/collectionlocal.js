var crypto             = require('crypto');
var bignum             = require('bignum');
var Query              = require('./query.js').Query;
var Logger             = require('./logger.js').Logger;
var collectionUtil     = require('./collectionutil');
var util               = require('util');

var CollectionLocal = exports.CollectionLocal = function(nodeName, name, loadHandler) {
	this.name = name;
	this.loadHandler = loadHandler;
	this.logger = new Logger("CollectionLocal[" + nodeName + "][" + this.name + "]");
	this.cached = {};
}

CollectionLocal.prototype.getName = function() {
	return this.name;
}

//P2P API

//Runs a query on the local data
CollectionLocal.prototype.queryData = function(json) {
	var q = new Query(json);
	this.logger.debug("Running query on local data: " + util.inspect(json));
	var result = [];
	for (var key in this.cached) {
		var entry = {
			'key' : key,
			'value' : this.cached[key].value,
			'hash' : this.cached[key].hash,
			'timestamp' : this.cached[key].timestamp
		}
	//	this.logger.debug("running query on entry: " + util.inspect(entry))
		if (q.execute(entry, this.name)) {
			result.push(entry);
		}
	}
	return result;
}


//Returns entry by key
CollectionLocal.prototype.getEntry = function(key) {
	this.logger.debug("Get Local Entry for key: " + key);
	if (!this.cached[key]) {
		this.logger.debug("Entry not found for key: " + key);
		return undefined;
	}
	return {
		'key' : key,
		'value' : this.cached[key].value,
		'hash' : this.cached[key].hash,
		'timestamp' : this.cached[key].timestamp
	}
}

//Returns a list of entries by start and end range
CollectionLocal.prototype.getData = function(start, end) {
	this.logger.debug("Get Local Data [" + start + ".." + end + "]");
	var localsInRange = [];
	var startNum = bignum(start);
	var endNum = bignum(end);
	for (var k in this.cached) {
		if (collectionUtil.valInRange(this.cached[k].hash, startNum, endNum)) {
			localsInRange.push(this.getLocalEntry(k));
		}
	}	
	return localsInRange;
}

//Adds entries to the local cache
CollectionLocal.prototype.addEntries = function(entries) {
	this.logger.debug("Add Local Entries " + entries);
	for (var e in entries) {
		this.addLocalEntry(e);
	}
}

//Adds an entry to the local cache
//If there is a key already added, checks the timestamp and ensures newer version of the value
CollectionLocal.prototype.addEntry = function(entry) {
	this.logger.debug("Add Local Entry " + entry);
	if (this.cached[entry.key] && entry.timestamp < this.cached[entry.key].timestamp) {
		this.logger.debug("We have more recent version of " + entry);
		return;
	}
	if (!this.cached[entry.key]) {
		this.loadHandler(1);
	}
	this.cached[entry.key] = {
		'value'     : entry.value,
		'hash'      : entry.hash,
		'timestamp' : entry.timestamp
	};
}
//Removes an entry by key
//Checks timestamp
CollectionLocal.prototype.removeEntry = function(key, timestamp) {
	this.logger.debug("Remove Local Entry; key: " + key + ", timestamp: " + timestamp);
	if (this.cached[key]) {
		if (timestamp > this.cached[key].timestamp) {
			this.logger.debug("Remove Local Entry; entry timestamp: " + this.cached[key].timestamp);
			delete this.cached[key];
			this.loadHandler(-1);
		}
	}
}
