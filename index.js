const { Kafka } = require("kafkajs");


const main = async () => {


  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ['localhost:9092']
  })

  const consumer = kafka.consumer({ groupId: "group-id" })

  await consumer.connect();

  await consumer.subscribe({
    topic: "topic2",
    fromBeginning: true
  })

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log({ message })
    }
  })
}

main()