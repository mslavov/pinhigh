var crypto             = require('crypto');
var bignum             = require('bignum');

exports.createEntry = function(key, value) {
	var hash = exports.getHashForKey(key);
	return {
		'key' : key,
		'value' : value,
		'hash' : hash,
		'timestamp' : new Date().getTime()
	}	
}

exports.valInRange = function(val, start, end) {
	return val.ge(start) && val.lt(end);
}

exports.getHashForKey = function(key) {	
	if (typeof(key) != 'string') {
		key = key.toString();
	}
	var hex = crypto.createHash('md5').update(key).digest("hex");
	return bignum(hex, 16);
}
