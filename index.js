/*
Handles connection to Rabbit MQ.
One AMQP Channel is created per TSP. (getAMQPChannel)
*/

const AMQP = require("amqplib");
const RETRY_TIMER = 5000; //ms

// opts for socket options and other attributes
class AMQPMgr {
    constructor(uri, opts) {
        this.uri = uri || "amqp://app:app@localhost:5672";
        this.is_started = false; 
        this.is_connected = false;
        if ( opts == null) opts = {};
        this.conn = null;
        this.conn_opts = opts || {};
        this.is_readonly = opts.readonly || false;
        this.id = 0;
        this.channels={};
        this.log = this.warn = this.err = this.dbg = () => {};
        return this;
    }
    //--------------------------------------------
    retryConnect()          { setTimeout(() => { this.connect(); } , RETRY_TIMER); }
    getChannelById(in_id)   { return this.channels[in_id]; }
    //--------------------------------------------
    async connect() {
        this.is_started = true;
        try {
            this.dbg("Trying to connect");
            this.conn = await AMQP.connect( this.uri );
            process.on('SIGINT', async ()  => { await this.conn.close(); process.exit(0); });
            this.conn.on("error", (err) =>  {
                this.warn("AMQP CON error : " + err);
            });
            this.conn.on("close", (err) =>  {
                this.warn("AMQP CON close : " + err);
                this.is_connected = false;
                this.retryConnect();
            });

            this.log("Connection Established");

        } catch (e) {
            this.error("Failed to establish Connection to RMQ")
            this.retryConnect();
            return;
        }
        this.is_connected = true;
        Object.keys(this.channels).forEach(id => {this.channels[id].connect();});
    }

    //--------------------------------------------
    async checkQueues(in_queues){
        if (! this.is_connected ){ 
            this.warn("No connection to AMQP yet");
            throw new Error("AMQP Connection is currently off");
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
        if ( ! this.is_started ) { await this.connect(); }
        let queues = [];
        if( typeof in_queues === "string") { queues.push(in_queues); } 
        else if (Array.isArray(in_queues)) { queues = in_queues.slice();}
        else {throw new Error("Invalid queues passed, expecting string or array of string");}
        // Before assigning a channel,  Check Queues existence
        try {
            await this.checkQueues(queues);
        } catch(e) {
            this.warn("Check queue failed on " + e.message )
            throw e;
        }
        let channel_id = this.id++;
        return this.channels[channel_id] = new AMQPTSPChannel(this, 
            { 
                id : channel_id, 
                queues : queues, 
                consumer_cb : in_handler,
                opts : {readonly : this.is_readonly}
            });
        
    }

} // end class AMQPMgr


// Channel Listener/Publisher
class AMQPTSPChannel {
    constructor(parent, opts) {
        this.parent = parent;
        this.id = opts.id;
        this.opts = opts;
        this.client_handler_fn = opts.consumer_cb;
        this.is_available = false;
        this.opts.stats = { publish : {ok : 0, errors:0}, recv : {ok : 0, errors:0} ,deconnection:0}
        this.connect();
        return this;
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
        } catch (e) {
            this.log("Channel establishment failed, rearm");
            this.retryConnect();
        }
    }
    //-------------------------------------------------------
    async publish( exchange, routing_key, payload, headers, properties) {
        if (! this.is_available) { throw new Error("No connection available");}
        if ( this.opts.readonly ) { throw new Error("Publishing is not authorized (readonly)");}
        if ( ! payload || typeof payload !== "object" ) { throw new Error("Invalid payload");}
        try {
            let data = Buffer.from(JSON.stringify(payload));
            let props = Object.assign({content_type : "application/json"},properties|| {});
            props.headers = headers || {};
            let res = await this.channel.publish(
                exchange, 
                routing_key, 
                data,
                props
            );
            this.dbg(`Message published to ${exchange} -> ${routing_key} `);
            this.opts.stats.publish.ok++;
            return res;
        } catch (e){
            this.opts.stats.publish.errors ++;
            this.warn(`FAILED to publish message to ${exchange} -> ${routing_key} : msg` + e.message);
            throw new Error("AMQP Publish error " + e.message);
        }
    }
}

module.exports = AMQPMgr;
