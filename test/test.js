var AMQP = require("../index.js");
var MyAmqpMgr = new AMQP();

MyAmqpMgr.log = console.log
MyAmqpMgr.warn= console.warn
MyAmqpMgr.error= console.error
MyAmqpMgr.dbg= console.debug
async function run(){
    try {
        let channel = await MyAmqpMgr.getChannel(["jedlix.queue", 'jedlix2.queue'], function(msg){ console.log(msg);});
        console.log("Listening : ");
        setTimeout(function(){
            channel.publish('toto.exchange',"test.message", {test:'ok', })
            .then(e => {console.log("Message Published, got" + e)})
            .catch(err => console.warn("Error" + err.message))
        }, 250);
    } catch (e) {
        console.log("Test Failed : " +e.message)
    }
}
run()

