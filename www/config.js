function getServer(endpoint) {
	var server = '/borders-api';
	return endpoint ? server + '/' + endpoint : server;
}
