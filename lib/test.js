var testFiles = [];
require("fs").readdirSync("./test/").forEach(function(file) {
	var patt = /.*\.test\..*/i;
	if (file.match(patt)) {
		if (!process.argv[2] || (process.argv[2] && file == process.argv[2])) {
			try {
				var s = require("../test/" + file);
				var tests = [];
				for (var t in s) {
					tests.push({
						'name' : t,
						'test' : s[t]
					});
				}
		  		testFiles.push({
					'name'  : file,
					'suite' : tests
				});
			} catch (e) {
				console.log("FAILED TO LOAD TEST: " + file);
				console.log(e);
			}
		}
	}
});

console.log("*********************************");
console.log("Test suites found:");
for (var i in testFiles) {
	console.log(" " + testFiles[i].name);
	for (var j in testFiles[i].suite) {
		console.log("   " + testFiles[i].suite[j].name);		
	}
}
console.log("*********************************");

var assert = function(test) {
	this.state = "init";
	this.test = test;	
};
assert.prototype.equal = function(expected, actual) {
	if (expected != actual) {
		throw "AssertError(" + expected + " != " + actual +")";
	}
}

assert.prototype.execute = function(callback) {
	this.state = "running";
	this.callback = callback;
	try {
		this.test(this);
	} catch (e) {
		this.endTest(e);
	}
}

assert.prototype.later = function(callback, t) {
	var self = this;
	if (this.state == "running") {
		setTimeout(function(){
			try {
				callback();
			} catch (e) {
				self.endTest(e);
			}
		}, t);
	}
}

assert.prototype.done = function(beCallback) {
	if (beCallback) {
		try {
			beCallback();
		} catch(e) {
			this.endTest(e);
			return;
		}
	}
	this.endTest();
}

assert.prototype.endTest = function(error) {
	this.state = "passed";
	if (error) {
		this.state = "failed";
		console.log("Test failed: " + error);
	}
	this.callback(this.state);
}

var currentSuite = 0;
var currentTest = 0;

var total = 0;
var passed = 0;

function nextTest(lastState) {
	if (lastState) {
		total ++;
		if (lastState == "passed") {
			passed ++;
		}
	}
	if (currentTest >= testFiles[currentSuite].suite.length) {
		currentSuite ++;
		currentTest = 0;
	}
	if (currentSuite < testFiles.length) {
		console.log("*********************************");
		console.log("Executing: " + testFiles[currentSuite].name + "[" + testFiles[currentSuite].suite[currentTest].name + "]");
		console.log("*********************************");
		var ass = new assert(testFiles[currentSuite].suite[currentTest].test);
		currentTest ++;		
		ass.execute(nextTest);
	} else {
		console.log("*********************************");
		console.log("Executed: " + total + ", passed: " + passed);
		console.log("*********************************");
	}
}
nextTest();

