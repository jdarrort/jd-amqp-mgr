/*
Handles connection to Rabbit MQ.
*/
const EventEmitter = require('events');
const AMQP = require("amqplib");
const DEFAULT_RECONNECT_DELAY = 15000; //ms

// opts for socket options and other attributes
class AMQPMgr extends EventEmitter {
    constructor(uri, opts) {
        super();
        this.uri = uri ;
        this.is_started = false; 
        this.is_connecting = false; 
        this.is_connected = false;
        if ( opts == null) opts = {};
        this.conn = null;
        this.conn_opts= {
            reconnect_delay : DEFAULT_RECONNECT_DELAY
        };
        this.conn_opts = Object.assign(this.conn_opts, opts) ;
        this.is_readonly = opts.readonly || false;
        this.id = 0;
        this.channels = {};
        this.pub_channel = null;
        this.log = this.warn = this.error = this.dbg = () => {};
        return this;
    }
    //--------------------------------------------
    retryConnect()          { setTimeout(() => { this.connect(); } , this.conn_opts.reconnect_delay); }
    getChannelById(in_id)   { return this.channels[in_id]; }
    //--------------------------------------------
    async connect() {
        this.is_connecting = true;
        let first_start = this.is_started ? false: true;
        try {
            this.dbg("Trying to connect");
            this.conn = await AMQP.connect( this.uri ,this.conn_opts);
            // Graceful shutdown
            process.on('SIGINT', ()  => { 
                if(this.is_connected) {
                    this.conn.close().then(e=>process.exit(0)).catch(e=>process.exit(1));
                }
            });
            this.conn.on("error", (err) =>  {
                this.warn("AMQP CON error : " + err);
            });
            this.conn.on("close", (err) =>  {
                this.warn("AMQP CON close : " + err);
                this.is_connected = false;
                this.emit('disconnected');
                this.retryConnect();
            });
            this.log("Connection Established");
            this.is_started = true;
        } catch (e) {
            this.error("Failed to establish Connection to RMQ")
            this.retryConnect();
            return;
        }
        this.is_connecting = false;
        this.is_connected = true;
        this.emit( first_start ? 'ready' : 'connected');
        Object.keys(this.channels).forEach(id => {this.channels[id].connect();});
    }

    //--------------------------------------------
    async checkQueues(in_queues){
        if (! this.is_connected ){ 
            this.warn("No connection to AMQP yet");
            throw new Error("AMQP Connection is currently off, retry later");
        }
        let tested_queues = []
        if (typeof in_queues === "string")  { tested_queues.push(in_queues)}
        else {tested_queues = in_queues;}
        //   Check Queues existence
        let test_channel;
        try {
            test_channel = await this.conn.createChannel();
        } catch(e) {
            this.warn("Channel creation failed " + e.message )
            throw new Error("Channel creation failed " + e.message )
        }
        // Declare handler so conn doesn't close upon failure to check queue existence
        test_channel.on("error", (err) => {});
        test_channel.on("close", (err) => {});
        for (let i in tested_queues) {
            try {
                await test_channel.checkQueue(tested_queues[i])
            } catch (e) {
                throw new Error(`CheckQueue failed for '${tested_queues[i]}' => ${e.code}`)
            }
        }
        test_channel.close();
        return true;
    }
    //--------------------------------------------
    // open a channel,to read/publish on certain queues/exchange
    async getChannel(in_queues, in_handler){
        if ( ! this.is_started && ! this.is_connecting ) { await this.connect(); }
        if (  ! this.is_started && this.is_connecting ) { 
            // Connection not yet available... should we queue request to later serve it?
            return new Promise((resolve, reject) => {
                this.log("getChannel for " + JSON.stringify(in_queues)+" is being queued until AMQP connection up")
                this.on('ready', () => {
                    this.getChannel(in_queues, in_handler).then(resolve).catch(reject);
                })
            })
        }
        let queues = []; 
        if( typeof in_queues === "string") { queues.push(in_queues); } 
        else if (Array.isArray(in_queues)) { queues = in_queues.slice();} // shallow copy
        else if (in_queues === null) { }
        else {throw new Error("Invalid queues passed, expecting string or array of string");}
        // Before assigning a channel,  Check Queues existence
        try {
            await this.checkQueues(queues);
        } catch(e) {
            this.warn("Check queue failed on " + e.message )
            throw e;
        }
        let channel_id = this.id++;
        try{
            return this.channels[channel_id] = await new AMQPTSPChannel( this, 
                { 
                    id : channel_id, 
                    queues : queues, 
                    consumer_cb : in_handler,
                    readonly : this.is_readonly
                });
        } catch(e) {
            this.warn("Failed to create Channel" + e.mess);
            throw e;
        }
    }
    //--------------------------------------------
    //removes a channel (to prevent from them to reconnect)
    removeChannel( in_channel_id ) {
        let channel = this.getChannelById(in_channel_id);
        if ( ! channel ) { throw new Error("Channel not found");}
        delete this.channels[in_channel_id];
        this.log(`Removed channel ${in_channel_id}`);

    }
    //--------------------------------------------
    // Publish on the pub channel
    async publish(exchange, routing_key, payload, headers, properties){
        let data = Buffer.from(JSON.stringify(payload));
        let props = Object.assign({contentType : "application/json"}, properties|| {});
        props.headers = headers || {};
        if (! this.is_connected ){ 
            this.warn("No connection to AMQP yet");
            throw new Error("AMQP Connection is currently off, retry later");
        }
        if (! this.pub_channel){
            this.pub_channel = await this.conn.createChannel();
            this.pub_channel.on('error', ()=>{})
            this.pub_channel.on('close', ()=>{ this.pub_channel=null;})
        }
        //   Check Queues existence
        this.dbg(`About to publish to ${exchange}/${routing_key} \n payload = ${JSON.stringify(payload)} \n properties = ${JSON.stringify(props)}`);
        this.pub_channel.publish(exchange, routing_key, data, props);
    }    
} // end class AMQPMgr

// Channel Listener/Publisher
class AMQPTSPChannel {
    constructor(parent, opts) {
        this.parent = parent;
        this.id = opts.id;
        this.opts = opts;
        this.queues = [];
        this.opts.queues.forEach(q => { this.queues.push( {name : q, consumer_tag : null, count:0})});
        this.client_handler_fn = opts.consumer_cb;
        this.is_initialized = false;
        this.is_available = false;
        this.is_deleted = false;
        this.is_paused = false;
        this.opts.stats = { publish : {ok : 0, errors:0}, recv : {ok : 0, errors:0} ,deconnection:0}
        return this.connect();
    }
    
    log(msg)  { this.parent.log (`AMQP-CH/${this.id} - ${msg}`) }
    dbg(msg)  { this.parent.dbg (`AMQP-CH/${this.id} - ${msg}`) }
    warn(msg) { this.parent.warn(`AMQP-CH/${this.id} - ${msg}`) }
    error(msg){ this.parent.error(`AMQP-CH/${this.id} - ${msg}`) }
    //-------------------------------------------------------
    getInfos() { return this.opts;}
    getQueue(in_queue_name) { return this.queues.find(q => q.name == in_queue_name);}
    deleteQueue(in_queue_name) { 
        let idx =  this.queues.findIndex(q => q.name == in_queue_name);
        if (idx >= 0) {this.queues.splice(idx, 1);}
    }
    //-------------------------------------------------------
    consumerFn( msg, from_queue){
        this.log(`Received ${msg.fields.routingKey}`);
        let queue = this.getQueue(from_queue); if (queue) { queue.count++;}
        let msg2;
        try {
            let props = {};
            Object.keys(msg.properties)
                .filter(p => (msg.properties[p] && p !== "headers"))
                .forEach(p => {props[p] = msg.properties[p]});
            let payload = msg.content.toString();
            try{
                payload = JSON.parse(payload);
            } catch( e ) { this.dbg("message payload is not JSON");}
            msg2 =  {
                exchange : msg.fields.exchange,
                queue : from_queue,
                routing_key : msg.fields.routingKey,
                headers : msg.properties.headers,
                payload : payload,
                properties : props
            }
            this.client_handler_fn( msg2 );
            this.channel.ack(msg); // ACK message
            this.opts.stats.recv.ok++;
            let q = this.queues.find( q => (q.name ==  from_queue));
            if (q) q.count++;
        } catch (e) {
            this.channel.nack(msg); // nack message
            this.opts.stats.recv.errors++;
            this.warn("Failed to process message " + JSON.stringify(msg));
        }
    }
    //-------------------------------------------------------
    async retryConnect() { setTimeout(() => { this.connect()}, DEFAULT_RECONNECT_DELAY);}
    async connect() {
        if ( ! this.parent.is_connected ) {
            this.log("No connection available,  will be rearmed when connection gets up again");
            return;
        }
        try {
            this.channel = await this.parent.conn.createChannel()
            this.channel.on("error", (err) => {
                this.warn("Event - Got  error : " + err);
            });
            this.channel.on("close", (err) => {
                this.is_available = false;
                // Rearm connection
                this.warn("Event -  Got  close : " + err);
                this.queues.forEach( q => {  q.consumer_tag = null;});
                this.opts.stats.deconnection ++;
                if (! this.is_deleted) {
                    this.retryConnect();
                }
            });
            this._consumeQueues();

            this.log("Channel  Established ");
            this.is_available = true;
            this.is_initialized = true;
            return this;
        } catch (e) {
            this.log("Channel establishment failed, rearm");
            if (this.is_initialized) {this.retryConnect();}
            else throw e;
        }
    }
    _consumeQueues(){
        let promises = [];
        this.queues.filter(q => (! q.consumer_tag && ! q.pause))
            .forEach( q => {
                let p = this.channel.consume( q.name, (msg)=>{ this.consumerFn(msg, q.name)})
                .then( (res) => {
                    this.log(`Starting consume on ${q.name} , consumer_tag ${res.consumerTag}`)
                    q.consumer_tag = res.consumerTag;
                promises.push(p);
                });
            });
        return Promise.all(promises);
    }

    //-------------------------------------------------------
    addQueues(new_queues) { // array of queues name
        if (! Array.isArray(new_queues)) { throw new Error("addQueues expect an array of queue names")}
        if ( ! this.is_available ) {
            throw new Error("Channel not available");
        }
        //check queues existence first.
        return this.parent.checkQueues(new_queues)
            .then( () => {
                new_queues.forEach( q => {
                    if ( this.getQueue(q) ){ 
                        this.warn(`Queue ${q} is already part of this channel`);
                        return;
                    }
                    this.queues.push({name : q, count:0, consumer_tag:null});
                });      
                return this._consumeQueues();
            })
            .catch(e => {
                this.error(`AddQueues failed ${e.message}`);
                throw e;
            });
    }
    //-------------------------------------------------------
    removeQueue(todelete_queue) { // array of queues name
        let queue = this.getQueue(todelete_queue);
        if (! queue) { 
            this.log("Nothing to remove");
            return true;
        }
        if ( ! this.is_available ) { // offline case
            this.log(`Removed queue ${todelete_queue} (offline) `)
            this.deleteQueue(todelete_queue)
            return true;
        } 
        // Otherwise, pause queues, and then delete them.
        return this.pause(todelete_queue)
            .then(() => {
                this.deleteQueue(todelete_queue)
                this.log(`Removed queue ${todelete_queue} (online) `)
            });
    }    
    //-------------------------------------------------------
    pause( in_queue ) { // pause consume on all(when no in_queue) or an a specific queue.
        let promises = [];
        let queues_to_pause = this.queues;
        if (in_queue) { 
            queues_to_pause = this.queues.filter(q => (q.name == in_queue));
        }
        queues_to_pause.forEach( q => { 
            q.pause = true;
            if ( q.consumer_tag ) {
                let p = this.channel.cancel( q.consumer_tag )
                .then( () => {
                    this.log( `Paused consume for queue  ${q.name} with consumer tag ${q.consumer_tag}` );
                    q.consumer_tag = null;
                })
                promises.push( p );
            }});
        return Promise.all( promises );
    }
    //-------------------------------------------------------
    resume(in_queue) { // pause consume on channel queues
        let queue = this.getQueue(in_queue);
        if (! queue) { throw new Error("Queue not defined for this channel");}
        if (! queue.pause == true) { 
            this.log("Queue is not currently paused");
            return true;
        }
        
        return this.channel.consume( queue.name, (msg)=>{ this.consumerFn(msg, queue.name)})
            .then( (res) => {
                this.log(`Resuming consume on ${queue.name} , consumer_tag ${res.consumerTag}`)
                queue.consumer_tag = res.consumerTag;
                queue.pause = false;
            });        
    }
    //-------------------------------------------------------
    publish( exchange, routing_key, payload, headers, properties) {
        if ( this.opts.readonly ) { throw new Error("Publishing is not authorized (readonly)");}
        this.opts.stats.publish.errors ++;
        return this.parent.publish(
                exchange, 
                routing_key, 
                payload,
                headers,
                properties
        );
    }    
    //-------------------------------------------------------
    close() { // Closes a Channel
        this.log("Channel is being closed");
        this.is_deleted = true;
        this.parent.removeChannel(this.opts.id);
        this.channel.removeAllListeners('close');
        this.channel.removeAllListeners('error');
        delete this.client_handler_fn;
        delete this.queues;
        this.channel.close();
        return true;
    }

}

module.exports = AMQPMgr;
