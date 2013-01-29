var Gossiper          = require('gossiper').Gossiper;
var bignum            = require('bignum');
var nconf             = require('nconf');
var PeerToPeer        = require('./p2p.js').PeerToPeer;
var Logger            = require('./logger.js').Logger;
var CollectionLocal   = require('./collectionlocal.js').CollectionLocal;
var CollectionProxy   = require('./collectionproxy.js').CollectionProxy;
var NodeConst         = require('./const.js').NodeConst;


var PinHigh = exports.PinHigh = function(name, hostname, gossipPort, dataPort, seeds, replicas) {
	//Setup defaults
	nconf.use('memory');
	nconf.set('pinhigh:name'      , 'pinhigh');
	nconf.set('pinhigh:gossipPort', 8000);
	nconf.set('pinhigh:dataPort'  , 9000);
	nconf.set('pinhigh:seeds'     , []);
	nconf.set('pinhigh:replicas'  , 0);
	nconf.set('pinhigh:config'    , 'config/pinhigh.json');
	nconf.set('logger:name'       , 'pinhigh');
	nconf.set('logger:mode'       , 'debug');

	//Setup env
	nconf.env();
	//Setup config file 
	if (nconf.get('pinhigh:config')) {
		nconf.file(nconf.get('pinhigh:config'));
	}
	//Configure
	this.gossipPort = getConfigValue(gossipPort, nconf.get('pinhigh:gossipPort'));
	this.dataPort = getConfigValue(dataPort, nconf.get('pinhigh:dataPort'));
	this.seeds = getConfigValue(seeds, nconf.get('pinhigh:seeds'));
	this.replicas = getConfigValue(replicas, nconf.get('pinhigh:replicas'));
	this.name = getConfigValue(name, nconf.get('pinhigh:name'));
	this.hostname = nconf.get('HOSTNAME');
	this.collections = {};
	this.cached = {};
	this.load = 0;
	this.state = "starting";
	Logger.configure(nconf.get('logger:name'), nconf.get('logger:mode'));
	this.logger = new Logger("PinHigh[" + this.name + "]");

}
//INIT
PinHigh.prototype.startP2P = function(callback) {
	this.logger.debug("Gossiper started");
	//Start P2P listener
	this.logger.debug("Starting p2p at port : " + this.dataPort);
	this.p2p = new PeerToPeer(this.dataPort, this);
	var self = this;
	this.p2p.start(function() {
		self.initialize(callback);
	});
}

PinHigh.prototype.initialize = function(callback) {
	//Intialize node
	this.logger.debug("Setting Application State to: " + this.state)
	this.gossiper.setLocalState(NodeConst.APP_STATE, this.state);
	this.gossiper.setLocalState(NodeConst.NODE_NAME, this.name);
	this.logger.debug("Setting Data Port to: " + [this.hostname, this.dataPort].join(':'))
	this.gossiper.setLocalState(NodeConst.DATA_PORT, [this.hostname, this.dataPort].join(':'));
	this.gossiper.setLocalState(NodeConst.LOAD, this.load);
	//By default set the token to the maximum.
	this.tokenEnd = bignum.pow(2, 128);
	this.tokenStart = bignum('0');
	if (this.seeds.length > 0) {
		this.logger.debug("Not the fist node in the cluseter");
		//Give some time to get the state of the other nodes.
		//After that figure out the token
		var self = this;
		this.logger.debug("Wait some time (30 * 1000) to get the gossiper updated");
		setTimeout(function() {
			//get the most loaded node and devide its space
			self.logger.debug("Get the most loaded peer");
			var peers = self.gossiper.allPeers();
			var load = -1;
			var mostLoadedPeer = null;
			for(var i in peers) {
				self.logger.debug("Peer: " + peers[i]);
				var peerLoad = self.gossiper.peerValue(peers[i], NodeConst.LOAD);
				self.logger.debug("Load for peer: " + self.gossiper.peerValue(peers[i], NodeConst.NODE_NAME) + " is " + peerLoad);
				if (load < peerLoad) {
					load = peerLoad;
					mostLoadedPeer = peers[i];
				}
			}
			var peerName = self.gossiper.peerValue(mostLoadedPeer, NodeConst.NODE_NAME);
			//Figure out the new start token for the max loaded peer and the new token range for this node
			var peerTokenStart = bignum(self.gossiper.peerValue(mostLoadedPeer, NodeConst.TOKEN_START));
			var peerTokenEnd = bignum(self.gossiper.peerValue(mostLoadedPeer, NodeConst.TOKEN_END));

			self.logger.debug("Got peer[" + peerName + "] with most load. From: " + peerTokenStart + " to:" + peerTokenEnd);

			var peerDataPort = self.gossiper.peerValue(mostLoadedPeer, NodeConst.DATA_PORT);
			var tokenRangeMiddle = peerTokenStart.add(peerTokenEnd).div(2);
			self.tokenStart = peerTokenStart;
			self.tokenEnd = tokenRangeMiddle;
			self.logger.debug("This node start: " + self.tokenStart + ", end: " + self.tokenEnd);
			self.gossiper.setLocalState(NodeConst.TOKEN_START, self.tokenStart.toString());
			self.gossiper.setLocalState(NodeConst.TOKEN_END, self.tokenEnd.toString());
			self.logger.debug("Sending new start token: " + tokenRangeMiddle + " to node: " + peerName);
			self.p2p.setNewStartToken(peerDataPort, tokenRangeMiddle.toString(), function() {
				self.gossiper.on('update', function(peer, k, v) {
					if (k == NodeConst.TOKEN_START && v == tokenRangeMiddle) {
						//Parent node updated its state, continue with getting the data
						self.logger.debug("Parent node updated its state, continue with getting the data");
						self.logger.debug("Getting data for range[" + self.tokenStart + ".." + self.tokenEnd + "]");
						self.p2p.getData(peerDataPort, self.tokenStart.toString(), self.tokenEnd.toString(), function(data) {
							self.logger.debug("Got data: " + data);
							self.addLocalEntries(data);
							self.logger.debug("Node ready!");
							self.logger.debug("===============================");
							self.state = "ready"
							self.gossiper.setLocalState(NodeConst.APP_STATE, self.state);				
							if (callback) callback();
						});				
					}
				});				
			});
		}, 30*1000);
	} else {
		this.logger.debug("First node in the cluster with range[" + this.tokenStart + ".." + this.tokenEnd + "]");
		this.gossiper.setLocalState(NodeConst.TOKEN_START, this.tokenStart.toString());
		this.gossiper.setLocalState(NodeConst.TOKEN_END, this.tokenEnd.toString());
		this.logger.debug("Node ready!");
		this.logger.debug("===============================");
		this.state = "ready"
		this.gossiper.setLocalState(NodeConst.APP_STATE, this.state);
		if (callback) callback();		
	}
}

//PUBLIC API
PinHigh.prototype.start = function(callback) {
	//Start Gossiper
	this.logger.debug("Starting gossip at port : " + this.gossipPort + " and seeds : " + this.seeds);
	this.gossiper = new Gossiper(this.gossipPort, this.seeds);
	var self = this;
	this.gossiper.start(function() {
		self.startP2P(callback);
	});
}

PinHigh.prototype.stop = function() {
	this.gossiper.stop();
	this.p2p.stop();
}

PinHigh.prototype.logger = function() {
	return this.logger;
}

PinHigh.prototype.collection = function(name) {
	return this.getOrCreateCollection(name).proxy;
}

PinHigh.prototype.getOrCreateCollection = function(name) {
	if (!this.collections[name]) {
		this.collections[name] = {};
		var self = this;
		this.collections[name].local = new CollectionLocal(this.name, name, function(add) {
			var load = self.gossiper.getLocalState(NodeConst.LOAD) + add;
			self.logger.debug("Updating load to " + load);
			self.gossiper.setLocalState(NodeConst.LOAD, load);		
			
		});
		this.collections[name].proxy = new CollectionProxy(this.name, name, this.replicas, this.p2p, this.gossiper);
	}
	return this.collections[name];	
}
PinHigh.prototype.allCollections = function() {
	return this.collections;
}

//P2P LISTENER CALLBACKS

//Updates the start token of the current node
//This happens when new node is introduced and decides to take over part of current node range.
PinHigh.prototype.newStartToken = function(token) {
	this.logger.debug("Got new start token: " + token);
	//before we update the token, wait to get msg from the new node
	var self = this;
	this.gossiper.on('update', function(peer, k, v) {
		//In this case we will still have overlaping ranges, but we can handle
		if (k == NodeConst.TOKEN_END && v == token) {
			self.tokenStart = bignum(token);
			self.gossiper.setLocalState(NodeConst.TOKEN_START, self.tokenStart.toString());	
		}
	});
}

PinHigh.prototype.addLocalEntries = function(collections) {
	this.logger.debug("Add Local Entries " + collections);
	for (var collectionName in collections) {
		var coll = getOrCreateCollection(collectionName);
		for (var i in collections[collectionName]) {
			coll.local.addEntry(collections[collectionName][i]);			
		}
	}
}

//PRIVATE API
function getConfigValue(arg, config) {
	var result = arg;
	if (!result) {
		result = config;
	}
	return result;
}
