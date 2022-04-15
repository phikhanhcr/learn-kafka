const { Kafka } = require('kafkajs')

const run = async () => {
  // Producing
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic: 'topic2',
    messages: [
      { value: 'Hello KafkaJS user!' },
    ],
  })
}

run().catch(console.error)