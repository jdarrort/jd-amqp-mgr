
# AMQP Connection Manager

Usage : 
var AmqpMgr = new require("jd-amqp-mgr")(uri, (opt)socket_opts)

Only to be used for 100% json payloads.

ex : 
var AmqpMgr = new require("jd-amqp-mgr")("amqp://guest:guest@localhost:5672")

**AmqpMgr** : 
    - 'checkQueues(in_queues)' : check existence of queues prior to create a channel (cause one invalid queue causes channel to be closed) 
    in_queues : array of string

    - 'getChannel(in_queues, callback_fn)' : check existence of queues prior to create a channel (cause one invalid queue causes channel to be closed) 
    in_queues : array of strings
    callback_fn : function invoked upon message reception.

    returns an instance of **AMQPTSPChannel**


**AMQPTSPChannel**
    - publish(exchange, routing_key, payload, headers,properties) : Publish a message 
