import express from "express"
import { Kafka } from "kafkajs";
import fs from "fs"
import dotenv from "dotenv";
const app = express();
dotenv.config();

const port = process.env.PORT ||  8000;

// Kafka setup
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [process.env.KAFKA_BROKER],
    ssl: {
      rejectUnauthorized: true, // Ensure certificates are checked
      ca: [fs.readFileSync('./ca.pem', 'utf-8')], // CA Certificate
      key: fs.readFileSync('./client-key.pem', 'utf-8'), // Client Key
      cert: fs.readFileSync('./client-cert.pem', 'utf-8'), // Client Certificate
    },
  });

// Producer
const producer = kafka.producer();

// Consumer
const consumer = kafka.consumer({ groupId: 'test-group' });

const runKafka = async () => {
  await producer.connect();
  await consumer.connect();

  // Send message
  await producer.send({
    topic: 'my-topic',
    messages: [{ value: 'Hello KafkaJS!' }],
  });

  // Subscribe to topic
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

  // Listen to messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
    },
  });
};

app.get('/', (req, res) => {
  res.send('Kafka producer and consumer running!');
});

app.listen(port, async () => {
  console.log(`Server is running on port ${port}`);
  await runKafka();
});
