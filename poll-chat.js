#!/usr/bin/node

 //
 // Very simple kafka based chat client for node JS
 //
 // Basically a less crappy functional test than the can-fake-with-netcat one that
 // comes with kafka
 //
 // David Basden <davidb@anchor.net.au>
 //

 "use strict";

 //============================================//

 var listenport = 31337;

 var kafkahost = '192.168.56.101';
 var kafkaport = 9092;
 var kafkatopic = 'test';
 var kafkapartition = 0;

 //============================================//

 var WebSocketServer = require('websocket').server;
 var http = require('http');

 // Logging
 function ln(msg) { console.log( (new Date()) + msg ); }

 // Setup HTTP listener
 var server = http.createServer(function(request,response) { });
 server.listen(listenport, function() {});


 // Websockets server
 //
 var wsServer = new WebSocketServer( { httpServer: server });
 wsServer.on('request', function(req) {
     var conn = req.accept(null,req.origin);
     var kafka = require('kafka-node');
     var Producer = kafka.Producer;
     var KeyedMessage = kafka.KeyedMessage;
     var Client = kafka.Client;
     var argv = require('optimist').argv;
     var topic = argv.topic || kafkatopic;
     var p = argv.p || 0;
     var a = argv.a || 0;

     // Handle messages from the client
     conn.on('message', function(msg) {
         if (msg.type === 'utf8') { // Only bother with text messages
             var data = msg.utf8Data;
             ln("Got message from client "+conn.remoteAddress+": " + data);
             // conn.sendUTF(data);
             //ln("echoed.");
             var keyedMessage = new KeyedMessage('keyed', 'a keyed message');
             var client = new Client('localhost:2181');
             var producer = new Producer(client, { requireAcks: 1 });

             producer.on('ready', function  () {
               producer.send([
                 { topic: topic, partition: p, messages: [data], attributes: a }
               ], function (err, result) {
                 //console.log(err || result);
                 //process.exit();
                 producer.close();
                 client.close();
               });
             });

         }
     });
     conn.on('error', function(err) { ln("websocketserver is throwing an error"); });

     var Consumer = kafka.Consumer;
     var Offset = kafka.Offset;
     var topics = [
         {topic: kafkatopic, partition: kafkapartition},
     ];
     var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
     var client1 = new Client('localhost:2181');
     var consumer = new Consumer(client1, topics, options);
     var offset = new Offset(client1);


     consumer.on('message', function (message) {
         ln(JSON.stringify(message.value));
         conn.sendUTF(message.value);
     });

     consumer.on('offsetOutOfRange', function (topic) {
       topic.maxNum = 2;
       offset.fetch([topic], function (err, offsets) {
         var min = Math.min(offsets[topic.topic][topic.partition]);
         consumer.setOffset(topic.topic, topic.partition, min);
       });
     });

     consumer.on('error', function(err,more) { ln("consumer is throwing an error"); ln(err.toString()); });

     // Handle client disconnections
     conn.on('close', function(conn) {
         ln(conn.remoteAddress +  " disconnected")
         ln("closing kafka consumer");
         consumer.close();
         client1.close();
     } );

 });