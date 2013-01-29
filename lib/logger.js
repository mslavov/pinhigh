var logly = require('logly');

var Logger = exports.Logger = function(name) {
	this.name = name;
}

Logger.configure = function(name, mode) {
	logly.name(name);
	logly.mode(mode);
	logly.options({ colour : true, date : true });	
}

Logger.prototype.info = function(msg) {
	logly.log(this.name + ": " + msg);
}

Logger.prototype.debug = function(msg) {
	logly.debug(this.name + ": " + msg);
}

Logger.prototype.warn = function(msg) {
	logly.warn(this.name + ": " + msg);
}

Logger.prototype.error = function(msg) {
	logly.error(this.name + ": " + msg);
}