"use strict";

var events = require("events");
var WebSocket = require("faye-websocket");
var EJSON = require("ddp-ejson");
var request = require('request');
var pathJoin = require('path').join;
var _ = require("ddp-underscore-patched");

class DDPClient extends events.EventEmitter {
  // internal stuff to track callbacks
  
  constructor (opts) {
    super()
    opts = opts || {};

    this._isConnecting = false;
    this._isReconnecting = false;
    this._nextId = 0;
    this._callbacks = {};
    this._updatedCallbacks = {};
    this._pendingMethods = {};
    this._observers = {};
    this._connectCallback = null;
  
    // backwards compatibility
    if ("use_ssl" in opts)              { opts.ssl = opts.use_ssl; }
    if ("auto_reconnect" in opts)       { opts.autoReconnect = opts.auto_reconnect; }
    if ("auto_reconnect_timer" in opts) { opts.autoReconnectTimer = opts.auto_reconnect_timer; }
    if ("maintain_collections" in opts) { opts.maintainCollections = opts.maintain_collections; }
    if ("ddp_version" in opts)          { opts.ddpVersion = opts.ddp_version; }
  
    // default arguments
    this.host = opts.host || "localhost";
    this.port = opts.port || 3000;
    this.path = opts.path;
    this.ssl  = opts.ssl  || this.port === 443;
    this.tlsOpts = opts.tlsOpts || {};
    this.useSockJs = opts.useSockJs || false;
    this.autoReconnect = ("autoReconnect" in opts) ? opts.autoReconnect : true;
    this.autoReconnectTimer = ("autoReconnectTimer" in opts) ? opts.autoReconnectTimer : 500;
    this.maintainCollections = ("maintainCollections" in opts) ? opts.maintainCollections : true;
    this.url  = opts.url;
    // support multiple ddp versions
    this.ddpVersion = ("ddpVersion" in opts) ? opts.ddpVersion : "1";
    this.supportedDdpVersions = ["1", "pre2", "pre1"];
  
    // Expose EJSON object, so client can use EJSON.addType(...)
    this.EJSON = EJSON;
    
    // very very simple collections (name -> [{id -> document}])
    if (this.maintainCollections) {
      this.collections = {};
    }
  }
  _prepareHandlers () {
    this.socket.on("open", () => {
      // just go ahead and open the connection on connect
      this._send({
        msg : "connect",
        version : this.ddpVersion,
        support : this.supportedDdpVersions
      });
    });
  
    this.socket.on("error", (error) => {
      // error received before connection was established
      if (this._isConnecting) {
        this._failed(error.message)
      }
  
      this.emit("socket-error", error);
    });
  
    this.socket.on("close", (event) => {
      this.emit("socket-close", event.code, event.reason);
      this._endPendingMethodCalls();
      this._recoverNetworkError();
    });
  
    this.socket.on("message", (event) => {
      this._message(event.data);
      this.emit("message", event.data);
    });
  };
  _clearReconnectTimeout () {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
  };
  
  _recoverNetworkError () {
    if (this.autoReconnect && ! this._isClosing) {
      this._clearReconnectTimeout();
      this.reconnectTimeout = setTimeout(() => { this.connect(); }, this.autoReconnectTimer);
      this._isReconnecting = true;
    }
  };
  
  ///////////////////////////////////////////////////////////////////////////
  // RAW, low level functions
  _send (data) {
    if (data.msg !== 'connect' && this._isConnecting) {
      this._endPendingMethodCalls()
    } else {
      this.socket.send(
        EJSON.stringify(data)
      );
    }
  };
  
  // handle a message from the server
  _message (data) {
  
    data = EJSON.parse(data);
  
    // TODO: 'addedBefore' -- not yet implemented in Meteor
    // TODO: 'movedBefore' -- not yet implemented in Meteor
  
    if (!data.msg) {
      return;
  
    } else if (data.msg === "failed") {
      if (this.supportedDdpVersions.indexOf(data.version) !== -1) {
        this.ddpVersion = data.version;
        this.connect();
      } else {
        this.autoReconnect = false;
        this._failed("Cannot negotiate DDP version")
      }
  
    } else if (data.msg === "connected") {
      this.session = data.session;
      this._wasConnected()
  
    // method result
    } else if (data.msg === "result") {
      var cb = this._callbacks[data.id];
  
      if (cb) {
        cb(data.error, data.result);
        delete this._callbacks[data.id];
      }
  
    // method updated
    } else if (data.msg === "updated") {
  
      _.each(data.methods, (method) => {
        var cb = this._updatedCallbacks[method];
        if (cb) {
          cb();
          delete this._updatedCallbacks[method];
        }
      });
  
    // missing subscription
    } else if (data.msg === "nosub") {
      var cb = this._callbacks[data.id];
  
      if (cb) {
        cb(data.error);
        delete this._callbacks[data.id];
      }
  
    // add document to collection
    } else if (data.msg === "added") {
      if (this.maintainCollections && data.collection) {
        var name = data.collection, id = data.id;
  
        if (! this.collections[name])     { this.collections[name] = {}; }
        if (! this.collections[name][id]) { this.collections[name][id] = {}; }
  
        this.collections[name][id]._id = id;
  
        if (data.fields) {
          _.each(data.fields, (value, key) => {
            this.collections[name][id][key] = value;
          });
        }
  
        if (this._observers[name]) {
          _.each(this._observers[name], (observer) => {
            observer.added(id, data.fields);
          });
        }
      }
  
    // remove document from collection
    } else if (data.msg === "removed") {
      if (this.maintainCollections && data.collection) {
        var name = data.collection, id = data.id;
  
        if (! this.collections[name][id]) {
          return;
        }
  
        var oldValue = this.collections[name][id];
  
        delete this.collections[name][id];
  
        if (this._observers[name]) {
          _.each(this._observers[name], (observer) => {
            observer.removed(id, oldValue);
          });
        }
      }
  
    // change document in collection
    } else if (data.msg === "changed") {
      if (this.maintainCollections && data.collection) {
        var name = data.collection, id = data.id;
  
        if (! this.collections[name])     { return; }
        if (! this.collections[name][id]) { return; }
  
        var oldFields     = {},
            clearedFields = data.cleared || [],
            newFields = {};
  
        if (data.fields) {
          _.each(data.fields, (value, key) => {
              oldFields[key] = this.collections[name][id][key];
              newFields[key] = value;
              this.collections[name][id][key] = value;
          });
        }
  
        if (data.cleared) {
          _.each(data.cleared, (value) => {
              delete this.collections[name][id][value];
          });
        }
  
        if (this._observers[name]) {
          _.each(this._observers[name], (observer) => {
            observer.changed(id, oldFields, clearedFields, newFields);
          });
        }
      }
  
    // subscriptions ready
    } else if (data.msg === "ready") {
      _.each(data.subs, (id) => {
        var cb = this._callbacks[id];
        if (cb) {
          cb();
          delete this._callbacks[id];
        }
      });
  
    // minimal heartbeat response for ddp pre2
    } else if (data.msg === "ping") {
      this._send(
        _.has(data, "id") ? { msg : "pong", id : data.id } : { msg : "pong" }
      );
    }
  };
  
  
  _getNextId () {
    return (this._nextId += 1).toString();
  };
  
  
  _addObserver (observer) {
    if (! this._observers[observer.name]) {
      this._observers[observer.name] = {};
    }
    this._observers[observer.name][observer._id] = observer;
  };
  
  
  _removeObserver (observer) {
    if (! this._observers[observer.name]) { return; }
  
    delete this._observers[observer.name][observer._id];
  };
  
  //////////////////////////////////////////////////////////////////////////
  // USER functions -- use these to control the client
  
  /* open the connection to the server
   *
   *  connected(): Called when the 'connected' message is received
   *               If autoReconnect is true (default), the callback will be
   *               called each time the connection is opened.
   */
  connect (cb) {
    this._isConnecting = true;
    this._connectionFailed = false;
    this._isClosing = false;
    
    this._clearOnConnectCallbacks()
    if (cb) {
      this._connectCallback = cb;
      
      this.once('connected', this._onConnectConnected);
      this.once('failed', this._onConnectFailed);
    }
  
    if (this.useSockJs) {
      this._makeSockJSConnection();
    } else {
      var url = this._buildWsUrl();
      this._makeWebSocketConnection(url);
    }
  }
  _onConnectConnected () {
    if (this._connectCallback) this._connectCallback(undefined, this._isReconnecting);
    this._clearOnConnectCallbacks()
  }
  _onConnectFailed (error) {
    if (this._connectCallback) this._connectCallback(error, this._isReconnecting);
    this._clearOnConnectCallbacks()
  }
  _clearOnConnectCallbacks () {
    this.removeListener('connected', this._onConnectConnected)
    this.removeListener('failed', this._onConnectFailed)
    this._connectCallback = null;
  }
  
  _endPendingMethodCalls () {
    var ids = _.keys(this._pendingMethods);
    this._pendingMethods = {};
  
    ids.forEach( (id) => {
      if (this._callbacks[id]) {
        this._callbacks[id](DDPClient.ERRORS.DISCONNECTED);
        delete this._callbacks[id];
      }
  
      if (this._updatedCallbacks[id]) {
        this._updatedCallbacks[id]();
        delete this._updatedCallbacks[id];
      }
    });
  };
  
  _makeSockJSConnection () {
  
    // do the info hit
    var protocol = this.ssl ? "https://" : "http://";
    var randomValue = "" + Math.ceil(Math.random() * 9999999);
    var path = pathJoin("/", this.path || "", "sockjs/info").replace(/\\/g,'/');
    var url = protocol + this.host + ":" + this.port + path;
  
    var requestOpts = { 'url': url, 'agentOptions': this.tlsOpts };
  
    request.get(requestOpts, (err, res, body) => {
      if (err) {
        this._recoverNetworkError();
      } else if (body) {
        var info;
        try {
          info = JSON.parse(body);
        } catch (e) {
          console.error(e);
        }
        if (!info || !info.base_url) {
          // no base_url, then use pure WS handling
          var url = this._buildWsUrl();
          this._makeWebSocketConnection(url);
        } else if (info.base_url.indexOf("http") === 0) {
          // base url for a different host
          var url = info.base_url + "/websocket";
          url = url.replace(/^http/, "ws");
          this._makeWebSocketConnection(url);
        } else {
          // base url for the same host
          var path = info.base_url + "/websocket";
          var url = this._buildWsUrl(path);
          this._makeWebSocketConnection(url);
        }
      } else {
        // no body. weird. use pure WS handling
        var url = this._buildWsUrl();
        this._makeWebSocketConnection(url);
      }
    });
  };
  
  _buildWsUrl (path) {
    var url;
    path = path || this.path || "websocket";
    var protocol = this.ssl ? "wss://" : "ws://";
    if (this.url && !this.useSockJs) {
      url = this.url;
    } else {
      url = protocol + this.host + ":" + this.port;
      url += (path.indexOf("/") === 0)? path : "/" + path;
    }
    return url;
  };
  
  _makeWebSocketConnection (url) {
    this.socket = new WebSocket.Client(url, null, this.tlsOpts);
    this._prepareHandlers();
  };
  
  close () {
    this._isClosing = true;
    this.socket.close();
    this.removeAllListeners("connected");
    this.removeAllListeners("failed");
  };
  
  _wasConnected () {
    this._clearReconnectTimeout();
    
    this._isConnecting = false;
    this._isReconnecting = false;

    this.emit("connected");
  }
  _failed (msg) {
    this._isConnecting = false;
    this._connectionFailed = true;
    
    this.emit('failed', msg)
  }
  
  // call a method on the server,
  //
  // callback = function(err, result)
  call (name, params, callback, updatedCallback) {
    var id = this._getNextId();
  
    this._callbacks[id] = (err, result) => {
      delete this._pendingMethods[id];
  
      if (callback) {
        callback.apply(this, [err, result]);
      }
    };
  
    this._updatedCallbacks[id] = (err, result) => {
      delete this._pendingMethods[id];
  
      if (updatedCallback) {
        updatedCallback.apply(this, [err, result]);
      }
    };
  
    this._pendingMethods[id] = true;
  
    this._send({
      msg    : "method",
      id     : id,
      method : name,
      params : params
    });
  };
  
  
  callWithRandomSeed (name, params, randomSeed, callback, updatedCallback) {
    var id = this._getNextId();
  
    if (callback) {
      this._callbacks[id] = callback;
    }
  
    if (updatedCallback) {
      this._updatedCallbacks[id] = updatedCallback;
    }
  
    this._send({
      msg        : "method",
      id         : id,
      method     : name,
      randomSeed : randomSeed,
      params     : params
    });
  };
  
  // open a subscription on the server, callback should handle on ready and nosub
  subscribe (name, params, callback) {
    var id = this._getNextId();
  
    if (callback) {
      this._callbacks[id] = callback;
    }
  
    this._send({
      msg    : "sub",
      id     : id,
      name   : name,
      params : params
    });
  
    return id;
  };
  
  unsubscribe (id) {
  
    this._send({
      msg : "unsub",
      id  : id
    });
  };
  
  /**
   * Adds an observer to a collection and returns the observer.
   * Observation can be stopped by calling the stop() method on the observer.
   * Functions for added, changed and removed can be added to the observer
   * afterward.
   */
  observe (name, added, changed, removed) {
    var observer = {};
    var id = this._getNextId();
  
    // name, _id are immutable
    Object.defineProperty(observer, "name", {
      get: () => { return name; },
      enumerable: true
    });
  
    Object.defineProperty(observer, "_id", { get: () => { return id; }});
  
    observer.added   = added   || (() => {});
    observer.changed = changed || (() => {});
    observer.removed = removed || (() => {});
  
    observer.stop = () => {
      this._removeObserver(observer);
    };
  
    this._addObserver(observer);
  
    return observer;
  };
};
DDPClient.ERRORS = {
  DISCONNECTED: new Error("DDPClient: Disconnected from DDP server")
};

module.exports = DDPClient;
