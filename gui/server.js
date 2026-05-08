const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Kafka } = require('kafkajs');
const protobuf = require('protobufjs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 3000;

app.use(express.static(path.join(__dirname, 'public')));

async function run() {
    // Load protobuf
    const root = await protobuf.load(path.join(__dirname, '../src/main/proto/messages.proto'));
    const MQuery = root.lookupType('com.example.kafkademo.MQuery');
    const MResponse = root.lookupType('com.example.kafkademo.MResponse');

    server.listen(PORT, () => {
        console.log(`GUI server running at http://localhost:${PORT}`);
    });

    const kafka = new Kafka({
        clientId: 'gui-monitor',
        brokers: ['localhost:9092']
    });

    const consumer = kafka.consumer({ groupId: 'gui-monitor-group' });

    try {
        await consumer.connect();
        await consumer.subscribe({ topics: ['Queries', 'Responses'], fromBeginning: false });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                let decodedMessage;
                try {
                    if (topic === 'Queries') {
                        decodedMessage = MQuery.decode(message.value);
                    } else if (topic === 'Responses') {
                        decodedMessage = MResponse.decode(message.value);
                    }

                    if (decodedMessage) {
                        const data = {
                            topic,
                            value: decodedMessage.toJSON(),
                            timestamp: message.timestamp
                        };
                        console.log(`[${topic}]`, data.value);
                        io.emit('kafka-message', data);
                    }
                } catch (err) {
                    console.error(`Error decoding message from ${topic}:`, err);
                }
            },
        });
    } catch (err) {
        console.error('Failed to connect to Kafka. Please ensure Kafka is running at localhost:9092');
        console.error('The GUI will still be available at http://localhost:3000, but no messages will be received.');
    }
}

run().catch(console.error);
