const { Kafka } = require("kafkajs");


const main = async () => {


  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ['localhost:9092']
  })

  const consumer = kafka.consumer({ groupId: "123" })

  await consumer.connect();

  await consumer.subscribe({
    topic: "user",
  })

  await consumer.run({
    eachMessage: async ({ message }) => {
      const parseMessage = JSON.parse(message.value.toString())
      console.log({ message, parseMessage })
    }
  })
}

main()