
var Query = exports.Query = function(json) {
	this.json = json;
}

Query.prototype.execute = function(entry, collection) {
	for (var i in this.json) {
		try {
			return this.handleTerm(i, this.json[i], entry, collection);
		} catch (e) {
			console.log(e);
			if (e == "notfound") {
				return false;				
			}
			throw e;
		}
	}
}

Query.prototype.handleTerm = function(key, value, entry, collection) {
	// console.log("-------------");
	// console.log("function: " + this[key]);
	// console.log("key: " + key);
	// console.log("value: " + value);
	// console.log("-------------");
	return this[key](value, entry, collection);
}

Query.prototype.and = function(value, entry, collection) {
	var res = true;
	for (var i in value) {
		res &= this.handleTerm(i, value[i], entry, collection);
	}
	return res;
}

Query.prototype.or = function(value, entry, collection) {
	var res = false;
	for (var i in value) {
		res |= this.handleTerm(i, value[i], entry, collection);
	}
	return res;
}

Query.prototype.eq = function(value, entry, collection) {
	var res = [];
	for (var i in value) {
		var result = this.handleTerm(i, value[i], entry, collection);
		for (var j in res) {
			if (res[j] != result) {
				return false;
			}
		}
		res.push(result);
	}
	return true;
}

Query.prototype.in = function(value, entry, collection) {
	var res = [];
	var range = [];
	for (var i in value) {
		if (i == 'range') {
			range = value[i];
		} else {
			res.push(this.handleTerm(i, value[i], entry, collection));
		}
		
	}
	for (var vr in res) {
		var found = false;
		for (var i in range) {
			if (res[vr] == range[i]) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

Query.prototype.collection = function(value, entry, collection) {
	return value == collection;
}

Query.prototype.val = function(val, entry, collection) {
	return val;
}

Query.prototype.key = function(value, entry, collection) {
	return entry.key;
}

Query.prototype.var = function(value, entry, collection) {
	var names = value.split('.');
	var current = entry;
	for (var i in names) {
		if (current == undefined) {
			throw 'notfound';
		}
		current = current[names[i]];
	}
	return current;
}