/*
Handles connection to Rabbit MQ.
One AMQP Channel is created per TSP. (getAMQPChannel)

To be initiated with 
new AMQPMgr(conn_opts) (conn_opts must contain an URI for amqp connection, like "amqp://guest:guest@localhost:5672")
*/

const AMQP = require("amqplib");
const RETRY_TIMER = 5000; //ms

// Each "TSP" have 1 channel, handled here.
class AMQPTSPChannel {
    constructor(parent, opts) {
        this.parent = parent;
        this.id = opts.id;
        this.opts = JSON.parse(JSON.stringify(opts)) || {};
        this.client_handler_fn = opts.consumer_cb;
        this.is_available = false;
        this.opts.stats = { publish : {ok : 0, errors:0}, recv : {ok : 0, errors:0},deconnection:0}
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
            this.opts.stats.recv.ok++
        } catch (e) {
            this.opts.stats.recv.errors++
            this.warn("Failed to process message");
        }
        this.channel.ack(msg); // Don't requeue
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
    async publish( exchange, routing_key, payload, headers) {
        if (! this.is_available) { throw new Error("No connection available");}
        try {
            let data = Buffer.from(JSON.stringify(payload));
            await this.channel.publish(
                exchange, 
                routing_key, 
                data,
                {
                    contentType : "application/json", 
                    headers : headers 
                }
            );
            this.dbg(`Message published to ${exchange} -> ${routing_key} `);
            this.opts.stats.publish.ok++
            return true;
        } catch (e){
            this.opts.stats.publish.errors++
            this.warn(`FAILED to publish message to ${exchange} -> ${routing_key}`);
            throw e;
        }
    }
}

class AMQPMgr {
    constructor(uri, socket_opts) {
        this.uri = uri;
        this.is_started = false; 
        this.is_connected = false;
        this.conn = null;
        this.id = 0;
        this.channels={};
        this.conn_opts = socket_opts || {};
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
            process.once('SIGINT', ()  => { this.conn.close(); });
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
        try {
            let test_channel = await this.conn.createChannel();
            // Declare handler so conn doesn't close upon failure to check queue existence
            test_channel.on("error", (err) => {});
            test_channel.on("close", (err) => {});
            let promises = [];
            in_queues.forEach( q => { promises.push(test_channel.checkQueue(q)); })
            await Promise.all(promises)
            test_channel.close();
        } catch(e) {
            this.warn("Check queue failed on " + e.message )
            throw e;
        }
    }
    //--------------------------------------------
    // open a channel,to read/publish on certain queues/exchange
    async getChannel(in_queues, in_handler){
        if ( ! this.is_started ) {await this.connect();}
        // Before assigning a channel,  Check Queues existence
        try {
            await this.checkQueues(in_queues);
        } catch(e) {
            this.warn("Check queue failed on " + e.message )
            throw e;
        }
        let channel_id = this.id++;
        return this.channels[channel_id] = new AMQPTSPChannel(this, { id : channel_id, queues : in_queues, consumer_cb : in_handler});
        
    }

} // end class AMQPMgr


module.exports = AMQPMgr;


