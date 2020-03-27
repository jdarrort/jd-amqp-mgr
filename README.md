
# AMQP Connection Manager
Wrapper of Amqplib  
It won't create any queue nor exchange, all are supposed to already exist.   
Handle reconnections and connection/channel/queues associations  
  
Usage : 
```javascript
var AMQP = require('jd-amqp-mgr');
var AmqpMgr = new AMQP("amqp://app:app@localhost:5672", opts);
// opts can contain socket_opts to be used for tls mutual auth with RMQ server
// opts.reconnect_delay : specify delay between reconnect attempts.

AmqpMgr.on('ready', function(){
  let channel = await AmqpMgr.getChannel(['queue1', 'queue2'], function(msg){
    console.log(msg);
  });

  channel.publish('my.exchange','my.routing.key',{test:1,is:'ok'},{hdr1:"somevalue"},{expiration:2000})
});
```
alternative 
```javascript
var AMQP = require('jd-amqp-mgr');

var AmqpMgr = new AMQP("amqp://app:app@localhost:5672");
AmqpMgr.getChannel('queue1', function(msg){ console.log(msg); })
.then(ch => { ch.publish('my.exchange','my.routing.key',{test:1,is:'ok'},{hdr1:"somevalue"},{expiration:2000}) })
.catch(e => {});

```

Only to be used for 100% json payloads.  
AMQP 0.9.1 

## **AmqpMgr** : 
- Manages automatic reconnections to server, and reconnect channels
- Emits a `ready` event upon first connection to RabbitMQ is established, and `reconnected`/`disconnected` afterwards
- `checkQueues(in_queues)` : check existence of queues prior to create a channel (cause one invalid queue causes channel to be closed) 
    `in_queues` : array of string

- `getChannel(in_queues, callback_fn)` : Create a channel that will consume message from queues in `in_queues`
  - returns a **Promise** that resolves with an instance of AMQPTSPChannel
  - `in_queues` : Queues that this channel will consume
    - array of strings
    - simple string
    - empty (if intended for publication only)
  - `callback_fn` : function invoked upon message reception.  
  - Request to `getChannel` will be queued until the first AMQP connection gets up. After first connection to AMQP, `getChannel` will fail if there is no ongoing connections.
- `removeChannel(in_channel_id)` : Remove a channel. Disconnection from this channel must be done directly over that channel.
- `publish(exchange, routing_key, payload, headers,properties)` : Publishes a message on an amqp channel dedicated to publishing.
  - `payload` is mandatory, object or array
  - `headers` and `properties` are optional. note that the property content_type will necessarily be set to `application/json`


## **AMQPTSPChannel**
- Represents a channel between the app and the RMQ server. Consumes on queues, and is able to publish message to exchanges. Automatic reconnection is covered.
- `publish(exchange, routing_key, payload, headers, properties)` : Publish a message over this channel
  - Returns a Promise
  - `payload`,`headers`,`properties` must be object
  - Property `content_type` is automatically set to `application/json`  
  - Throw error if invalid attributes.
  - Failure to send the message (invalid exchange as an example) will not end up with an error being triggered related to the `publish` call, but the amqp server will close the Channel. The close event on a channel triggers the reconnection.
- `addQueues(in_queues)` : Start consuming messages from that queue
- `removeQueue(in_queue)` : stop consuming a queue.
- `pause(in_queue)` : pause consuming a queue.
- `resume(in_queue)` : resume consuming a queue.
- `close()` : Ends this channel
