const zmq = require('zeromq');

const sock = zmq.socket('sub');

sock.connect('tcp://eddn.edcd.io:9500');

sock.subscribe('');

exports.sock = sock;
