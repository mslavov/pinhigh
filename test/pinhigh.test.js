var PinHigh = require('pinhigh').PinHigh;

module.exports[ 'should be able to put and get value'] = function(assert) {
	var val = null;
    var pinHigh = new PinHigh();
	pinHigh.start(function(){});
	var logger = console.log;
	assert.later(function(){
		logger = pinHigh.logger.info;
		logger("PinHigh started");
		pinHigh.collection('test').put("key", "val", function(){});
	}, 2000);
	
	assert.later(function(){
		logger("'put' completed");
		pinHigh.collection('test').get("key", function(data){
			logger("'get' completed: " + data);
			val = data;
		});
	}, 4000);
	
	assert.later(function(){		
		pinHigh.stop();
		assert.done(function(){
		    assert.equal('val', val);					
		});
	}, 6000);	
}