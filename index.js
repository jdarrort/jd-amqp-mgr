/*
Handles connection to Rabbit MQ.
*/
const EventEmitter = require('events');
const AMQP = require("amqplib");
const RETRY_TIMER = 5000; //ms

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
        this.conn_opts = opts || {};
        this.is_readonly = opts.readonly || false;
        this.id = 0;
        this.channels = {};
        this.pub_channel = null;
        this.log = this.warn = this.error = this.dbg = () => {};
        return this;
    }
    //--------------------------------------------
    retryConnect()          { setTimeout(() => { this.connect(); } , RETRY_TIMER); }
    getChannelById(in_id)   { return this.channels[in_id]; }
    //--------------------------------------------
    async connect() {
        this.is_connecting = true;
        let init_state = this.is_started;
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
        if (!init_state) {this.emit('ready');}
        Object.keys(this.channels).forEach(id => {this.channels[id].connect();});
    }

    //--------------------------------------------
    async checkQueues(in_queues){
        if (! this.is_connected ){ 
            this.warn("No connection to AMQP yet");
            throw new Error("AMQP Connection is currently off, retry later");
        }
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
        for (let i in in_queues) {
            try {
                await test_channel.checkQueue(in_queues[i])
            } catch (e) {
                throw new Error(`CheckQueue failed for '${in_queues[i]}' => ${e.code}`)
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
        else if (Array.isArray(in_queues)) { queues = in_queues.slice();}
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
                    opts : {readonly : this.is_readonly}
                });
        } catch(e) {
            this.warn("Failed to create Channel" + e.mess);
            throw e;
        }
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
        let tmpchan;
        this.pub_channel.publish(exchange, routing_key, data, props);
    }    
} // end class AMQPMgr

// Channel Listener/Publisher
class AMQPTSPChannel {
    constructor(parent, opts) {
        this.parent = parent;
        this.id = opts.id;
        this.opts = opts;
        this.client_handler_fn = opts.consumer_cb;
        this.is_initialized = false;
        this.is_available = false;
        this.opts.stats = { publish : {ok : 0, errors:0}, recv : {ok : 0, errors:0} ,deconnection:0}
        return this.connect();
    }
    log(msg)  { this.parent.log (`AMQP-CH/${this.id} - ${msg}`) }
    dbg(msg)  { this.parent.dbg (`AMQP-CH/${this.id} - ${msg}`) }
    warn(msg) { this.parent.warn(`AMQP-CH/${this.id} - ${msg}`) }
    error(msg){ this.parent.error(`AMQP-CH/${this.id} - ${msg}`) }
    //-------------------------------------------------------
    consumerFn(msg){
        this.log(`Received ${msg.fields.routingKey}`);
        this.channel.ack(msg); // Don't requeue
        let msg2;
        try {
            let props = {};
            Object.keys(msg.properties)
                .filter(p => (msg.properties[p] && p !== "headers"))
                .forEach(p => {props[p] = msg.properties[p]});

            msg2 =  {
                exchange : msg.fields.exchange,
                routing_key : msg.fields.routingKey,
                headers : msg.properties.headers,
                payload : JSON.parse(msg.content.toString()),
                properties : props
            }
            this.client_handler_fn( msg2 );
            this.opts.stats.recv.ok++;
        } catch (e) {
            this.opts.stats.recv.errors++;
            this.warn("Failed to process message " + JSON.stringify(msg));
        }
    }
    //-------------------------------------------------------
    getInfos() { return this.opts;}
    async retryConnect() { setTimeout(() => { this.connect()}, RETRY_TIMER);}
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
                this.opts.stats.deconnection ++;
                this.retryConnect();
            });
            this.opts.queues.forEach(q => {
                this.channel.consume(q, (msg)=>{this.consumerFn(msg)});
            });
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
    //-------------------------------------------------------
    publish( exchange, routing_key, payload, headers, properties) {
        if ( this.opts.readonly ) { throw new Error("Publishing is not authorized (readonly)");}
        this.opts.stats.publish.errors ++;
        return this.parent.publish(
                exchange, 
                routing_key, 
                data,
                props
        );
    }
}

module.exports = AMQPMgr;
