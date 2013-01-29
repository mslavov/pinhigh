var PinHigh = require('pinhigh').PinHigh;

module.exports[ 'two nodes'] = function(assert) {
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
			pinHigh.collection('test').put("key", "val", function(){
				logger("'put' completed");
				pinHigh1.collection('test').get("key", function(data){
					logger("'get' completed: " + data);
					var val = data;
					pinHigh.collection('test').remove("key", function(){
						assert.later(function(){		
							pinHigh1.stop();
							pinHigh.stop();
							assert.done(function(){
							    assert.equal('val', val);				
							});							
						}, 1000);
						
					});
				});				
			});
		});		
	});	
}
