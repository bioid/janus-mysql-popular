var mysql  = require('mysql');
var args   = require('optimist').argv;
var config = require(args.config || '../../config.js');

var create_popular_query = "CREATE TABLE IF NOT EXISTS `popular` (";
    create_popular_query+= "`id` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(`id`),";
    create_popular_query+= "  `url` varchar(255) NOT NULL, UNIQUE(`url`),";
    create_popular_query+= "  `roomName` varchar(255) DEFAULT NULL,";
    create_popular_query+= "  `count` int(11) DEFAULT 1";
    create_popular_query+= ")";


function Plugin() {
    console.log("Loading janus-mysql-popular");
    log.info("Loading janus-mysql-popular");
    this._conn = mysql.createPool({
    host     : config.MySQL_Hostname,
    user     : config.MySQL_Username,
    password : config.MySQL_Password,
    database : config.MySQL_Database
    });

    this._conn.query(create_popular_query, function(err, results) {
        if(err != null) throw new Error(err);
        if(results.warningCount == 0) log.info("Created `popular` table.");
    });

    console.log("Connected to mysql server "+config.MySQL_Hostname);
    log.info("Connected to mysql server "+config.MySQL_Hostname);
}

Plugin.prototype.call = function(name, data) {
    if (!data.roomUrl)
      return;
    var url = data.roomUrl,
        roomName = data.roomName || "";

    var sql = "INSERT INTO `popular` (url, roomName) VALUES (?, ?) ON DUPLICATE KEY UPDATE count = count + 1;";
    this._conn.query(sql, [url, roomName], function(err, results) {
        if(err != null) console.log(err);
    });
}


module.exports = new Plugin();
