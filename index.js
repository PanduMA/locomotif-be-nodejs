import bodyParser from "body-parser";
import express from "express";
import controllers from "./controller.js";
import KafkaConfig from "./config.js";
import mongoose from "mongoose";

const app = express();
const jsonParser = bodyParser.json();

//api post data to kafka
app.post("/api/send", jsonParser, controllers.sendMessageToKafka)

// consume message from kafka
const kafkaConfig = new KafkaConfig()
kafkaConfig.consume('info-lokomotif', (value) => {
    console.log(value);
});

//conection to mongodb
mongoose.connect('mongodb://localhost:27017/locomongo-db', { useNewUrlParser: true, useUnifiedTopology: true });
const db = mongoose.connection;

db.on('error', console.error.bind(console, 'Failed connection to MongoDB'));
db.once('open', function() {
    console.log('Connected to MongoDB');
})

app.listen(8080, () => {
    console.log("Server running in port 8080.");
})