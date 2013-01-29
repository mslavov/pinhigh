var Query   = require('pinhigh').Query;
var PinHigh = require('pinhigh').PinHigh;
var util    = require('util');

var entries = [
	{
		'value':
			{
				'first' : 'Milko',
				'last' : 'Slavov'			
			},
		'key' : 1
	},
	{
		'value':
			{
				'last' : 'Slavov'			
			},
		'key' : 2
	}
];


module.exports['Local: test equal'] = function(assert) {
	var q = new Query({
		'eq' : {
			'var' : 'value.first',
			'val' : 'Milko'
		}
	});
	assert.equal(true, q.execute(entries[0], 'test'));
	assert.equal(false, q.execute(entries[1], 'test'));
	assert.done();
}

module.exports['Local: test equal and collection'] = function(assert) {
	var q = new Query({
		'and' : {
			'collection' : 'test',
			'eq' : {
				'var' : 'value.first',
				'val' : 'Milko'
			}
		}
	});
	assert.equal(true, q.execute(entries[0], 'test'));
	assert.done();
}

function testQuery(query, assert, assertCallback) {
    var pinHigh = new PinHigh();
	pinHigh.start(function(){
		var logger = pinHigh.logger.info;
		logger("PinHigh1 started");
	    pinHigh1 = new PinHigh("node2", 
						   "Milkos-MacBook-Pro.local",
							8001,
							9001,
							["192.168.1.106:8000"],
							1);
		pinHigh1.start(function(){
			logger("PinHigh2 started");
			pinHigh.collection('test').put("key1", entries[0].value, function(){
				logger("'put'1 completed");
				pinHigh.collection('test').put("key2", entries[1].value, function(){
					logger("'put'2 completed");
					pinHigh1.collection('test').query(query, function(data){
						logger("'query' completed" + util.inspect(data));
						assertCallback(data);
						assert.later(function(){		
							pinHigh1.stop();
							pinHigh.stop();
							assert.done();							
						}, 1000);
					});
				});				
			});
		});		
	});		
}

module.exports['2 Nodes: test equal'] = function(assert) {
	var q = {
		'eq' : {
			'var' : 'value.first',
			'val' : 'Milko'
		}
	};
	testQuery(q, assert, function(data){
		assert.equal(1, data['test'].length);
	    assert.equal(entries[0].value.first, data['test'][0].first);
	});
}

module.exports['2 Nodes: key in range'] = function(assert) {
	var q = {
		'in' : {
			'var' : 'key',
			'range' : ["key1","key2","key3"]
		}
	};
	testQuery(q, assert, function(data){
		assert.equal(2, data['test'].length);
	    assert.equal(entries[0].value.first, data['test'][0].first);
	});
}


