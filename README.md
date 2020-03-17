
# AMQP Connection Manager

Usage : 
```javascript
var AMQP = require('jd-amqp-mgr');
var AmqpMgr = new AMQP(uri, socket_opts);
// socket_opts to be used to pass tls attributes

AmqpMgr.getChannel(['queue1', 'queue2'], function(msg) {console.log(msg);})

AmqpMgr.publish('my.exchange','my.routing.key',{test:1,is:'ok'},{hdr1:"somevalue"},{expiration:2000})
```

Only to be used for 100% json payloads.

Manages connection automatic retries

## **AmqpMgr** : 

- `checkQueues(in_queues)` : check existence of queues prior to create a channel (cause one invalid queue causes channel to be closed) 
    in_queues : array of string

- `getChannel(in_queues, callback_fn)` : Create a channel that will consume message from queues in `in_queues`
  - returns an instance of AMQPTSPChannel
  - `in_queues` : array of strings, or simple string
  - `callback_fn` : function invoked upon message reception.  
  All messages will be acked, even if processing fails.

## **AMQPTSPChannel**
- Represents a channel between the app and the RMQ server. Consumes on queues, and is able to publish message to exchanges. Automatic reconnection is covered.
- `publish(exchange, routing_key, payload, headers, properties)` : Publish a message.  
  - `payload`,`headers`,`properties` must be object
  - Property `content_type` is automatically set to `application/json`  
  - Throw error if invalid attributes.
  - Failure to send the message (invalid exchange as an example) will not end up with an error being triggered related to the `publish` call, but the amqp server will close the Channel. The close event on a channel triggers the reconnection.
