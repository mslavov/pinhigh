
var QueryResultHandler = exports.QueryResultHandler = function(callback) {
	this.callback = callback;
	this.requestCount = 0;
	this.responseCount = 0;
	this.result = {};
}

QueryResultHandler.prototype.queryHandler = function() {
	this.requestCount ++;
	var self = this;
	return function(data, error, end) {
		self.responseCount ++;
		if (data) {
			for (var colName in data) {
				if (!self.result[colName]) {
					self.result[colName] = [];
				}
				for (var e in data[colName]) {
					var shouldAdd = true;
					for (var r in self.result[colName]) {
						//if we already have value for the same key in the result list we have to check the timestamp
						if (self.result[colName][r].key == data[colName][e].key) {
							shouldAdd = false;
							if (self.result[colName][r].timestamp < data[colName][e].timestamp) {
								self.result[colName][r] = data[colName][e];
							}
							break;
						}
					}
					if (shouldAdd) {
						self.result[colName].push(data[colName][e]);
					}
				}
			}
		}
		if (self.requestCount == self.responseCount) {
			var values = {};
			for (var colName in self.result) {
				if (!values[colName]) {
					values[colName] = [];
				}
				for (var e in self.result[colName]) {
					values[colName].push(self.result[colName][e].value)
				}
			}
			self.callback(values);
		}
	}
}
