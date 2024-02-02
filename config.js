import { Kafka } from "kafkajs";
import mongoose from "mongoose";

class KafkaConfig {
    constructor() {
        this.kafka = new Kafka({
            clientId: "locomotif-kafka",
            brokers: ['localhost:9092']
        });
        this.producer = this.kafka.producer();
        this.consumer = this.kafka.consumer({ groupId: "locoGroup" });
    }

    async produce(topic, message) {
        try {
            await this.producer.connect();
            const messages = [{
                value: JSON.stringify(message),
            }, ];
            await this.producer.send({
                topic: topic,
                messages: messages
            });
        } catch (error) {
            console.error(error);
        } finally {
            await this.producer.disconnect()
        }
    }

    async consume(topic, callback) {
        try {
            await this.consumer.connect();
            await this.consumer.subscribe({ topic: topic, fromBeginning: true });
            await this.consumer.run({
                eachMessage: async({ topic, partition, message }) => {
                    const value = JSON.parse(message.value.toString());
                    if (!mongoose.models['locomotif-model']) {
                        const schema = new mongoose.Schema({
                            code: String,
                            name: String,
                            dimension: String,
                            status: String,
                            date: String
                        });

                        mongoose.model('locomotif-model', schema);
                    }

                    const locoModel = mongoose.model('locomotif-model');
                    const data = new locoModel({
                        code: value.code,
                        name: value.name,
                        dimension: value.dimension,
                        status: value.status,
                        date: value.date
                    })
                    data.save().then(() => {
                        console.log('Success save data to MongoDB');
                    }).catch((error) => {
                        console.error('Failed save data to MongoDB : ' + error)
                    })
                    callback(value);
                },
            });
        } catch (error) {
            console.error(error);
        }
    }
}

export default KafkaConfig;