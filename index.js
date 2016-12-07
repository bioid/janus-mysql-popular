var mysql  = require('mysql');

var create_popular_query = "CREATE TABLE IF NOT EXISTS `popular` (";
    create_popular_query+= "`id` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY(`id`),";
    create_popular_query+= "  `url` varchar(255) NOT NULL, UNIQUE(`url`),";
    create_popular_query+= "  `roomName` varchar(255) DEFAULT NULL,";
    create_popular_query+= "  `count` int(11) DEFAULT 1,";
    create_popular_query+= "  `weight` DECIMAL(11, 4) DEFAULT NULL,";
    create_popular_query+= "  `lastEvaluated` DATETIME,";
    create_popular_query+= "  `lastSeen` DATETIME";

    create_popular_query+= ")";

var drop_update_procedure = "DROP PROCEDURE IF EXISTS updatePopularWeight;"
var create_update_procedure = ""
    create_update_procedure += "CREATE PROCEDURE `updatePopularWeight`(IN `halfLife` INT(11))" 
    create_update_procedure += "BEGIN"
    create_update_procedure += "  UPDATE `popular` "
    create_update_procedure += "    SET weight = weight * POW(2,"
    create_update_procedure += "    (TIMESTAMPDIFF(SECOND, lastEvaluated, NOW()) * 1000) / halfLife),"
    create_update_procedure += "    lastEvaluated = NOW()"
    create_update_procedure += "    WHERE id like '%';"
    create_update_procedure += "END"

function Plugin() {
    console.log("Loading janus-mysql-popular");
    log.info("Loading janus-mysql-popular");
    this._conn = mysql.createPool({
        host     : config.MySQL_Hostname,
        user     : config.MySQL_Username,
        password : config.MySQL_Password,
        database : config.MySQL_Database
    });
    this._updateInterval = global.config.popularRooms.updateInterval || 300000;
    this._halfLife = global.config.popularRooms.halfLife || 7 * 24 * 60 * 60 * 1000;
    this._lastUpdate = Date.now();

    this._conn.query(create_popular_query, function(err, results) {
        if(err != null) throw new Error(err);
        if(results.warningCount == 0) log.info("Created `popular` table.");
        this._conn.query(drop_update_procedure, function(err) {
            if (err) throw new Error(err);
            this._conn.query(create_update_procedure, function(err) {
              if (err) throw new Error(err);
            })
        }.bind(this));
    }.bind(this));

    console.log("Connected to mysql server "+config.MySQL_Hostname);
    log.info("Connected to mysql server "+config.MySQL_Hostname);
}

Plugin.prototype.call = function(name, data) {
    if (!data.roomUrl || data.roomUrl === "http://www.janusvr.com/newlobby/index.html"
        ||  data.roomUrl === "http://www.janusvr.com/index.html")
      return;
    var url = data.roomUrl,
        roomName = data.roomName || "";

    var sql = "INSERT INTO `popular` (url, roomName, weight, lastEvaluated, lastSeen) "; 
        sql += "VALUES (?, ?, 2.0, NOW(), NOW()) ";
        sql += "ON DUPLICATE KEY UPDATE count = count + 1,"
        sql += " weight = 1 + (weight * POW(2, (TIMESTAMPDIFF(SECOND, NOW(), lastEvaluated) * 1000) / ?)),"
        sql += " lastSeen = NOW();";
    this._conn.query(sql, [url, roomName, this._halfLife], function(err, results) {
        if(err != null) console.log(err);
        var now = Date.now();
        if (now - this._lastUpdate >= this._updateInterval) {
          this._conn.query("CALL updatePopularWeight(?);", [this._halfLife], function(err) {
            if (err) console.log(err);
          });
        }
    }.bind(this));
}


module.exports = new Plugin();
