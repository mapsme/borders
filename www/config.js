window.BYTES_FOR_NODE = 8;

function getServer(endpoint) {
	var server = '/borders-api';
	return endpoint ? server + '/' + endpoint : server;
}
