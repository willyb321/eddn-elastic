require('dotenv').config()
const zlib = require("zlib");
const camelcaseKeys = require("camelcase-keys");
const { promisify } = require("util");
const zmq = require("zeromq");
const { Client: ElasticClient } = require("@elastic/elasticsearch");
const sock = zmq.socket("sub");
const pInflate = promisify(zlib.inflate);
const { logger } = require("./log");

sock.connect("tcp://eddn.edcd.io:9500");
console.log("Worker connected to port 9500");
const eClient = new ElasticClient({
  node: process.env.ELASTIC_HOST,
  maxRetries: 5,
  requestTimeout: 60000,
  sniffOnStart: true,
  auth: {
    username: process.env.ELASTIC_USER,
    password: process.env.ELASTIC_PASSWORD
  }
});

sock.subscribe("");

sock.on("message", processMessage);

async function processMessage(message) {
  let jsonString;
  try {
    jsonString = await pInflate(message);
  } catch (error) {
    logger.warn(
      `Error processing raw data to string @ ${new Date().toISOString()}`,
      error
    );
    return;
  }

  if (!jsonString) {
    return;
  }
  let json;
  try {
    json = JSON.parse(jsonString);
  } catch (error) {
    logger.warn(
      `Error processing json string to object @ ${new Date().toISOString()}`,
      error
    );
    return;
  }

  if (!json) {
    return;
  }

  try {
    json = addComputedFields(json);
  } catch (error) {
    logger.warn(
      `Error adding extra fields to object @ ${new Date().toISOString()}`,
      error
    );
    return;
  }

  if (!json) {
    return;
  }

  try {
    await eClient.index({
      index: `eddn-${json.extra.schema || "unknown"}`,
      //type: '_doc', // uncomment this line if you are using Elasticsearch ≤ 6
      body: json
    });
  } catch (error) {
    logger.warn("Error indexing message to Elastic", error);
    return;
  }

  if (!res) {
    return;
  }
}

function addComputedFields(message) {
  if (!message || !message.header || !message.message) {
    return;
  }

  // Initialise an extra object on message
  if (!message.extra) {
    message.extra = {};
  }

  // Add a combined software name and version field
  const softwareKey = `${message.header.softwareName}@${message.header.softwareVersion}`;
  message.extra.softwareKey = softwareKey;

  // Camel case root level keys for consistency
  message = camelcaseKeys(message);
  message.header = camelcaseKeys(message.header);
  message.message = camelcaseKeys(message.message);

  // Add a simplified schema field
  if (message.$schemaRef.toString().indexOf("journal") !== -1) {
    message.extra.schema = "journal";
  } else if (message.$schemaRef.toString().indexOf("commodity") !== -1) {
    message.extra.schema = "commodity";
  } else if (message.$schemaRef.toString().indexOf("outfitting") !== -1) {
    message.extra.schema = "outfitting";
  } else if (message.$schemaRef.toString().indexOf("shipyard") !== -1) {
    message.extra.schema = "shipyard";
  }
  if (message.$schemaRef.toString().indexOf("test") !== -1) {
    message.extra.schema = "test";
  }

  return message;
}