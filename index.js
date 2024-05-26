'use strict';
const { driverPrototype } = require('use-db');
const sqlite3 = require('sqlite3');


// public
const transactionIsolationLevels = {
  def: 'DEFERRED',
  imm: 'IMMEDIATE',
  exc: 'EXCLUSIVE'
};



// Options = {
//    filename: '<path>'|':memory:'
//    mode
// }
function Sqlite3(options) {
  let logger;

  this. db = null;


  this.txIsolationLevels = transactionIsolationLevels;
  this.timestamp = (ms) => (ms==null ? new Date() : new Date(ms)).getTime();


  this.initialize = function(opts = {}) {
    logger = opts.logger || this.logger;

    return new Promise((resolve, reject) => {
      sqlite3.verbose();

      let fnHandle = (err) => {
        if (err){
          reject(err);
        }
        else {
          resolve();
        }
      };

      let mode = options.mode; 
      if (mode == null){
        mode = fnHandle;
        fnHandle = undefined;
      }

      this.db = new sqlite3.Database(options.filename, mode, fnHandle);
    });
  }

  this.shutdown = function() {
    return new Promise((resolve, reject) => {
      if (this.db != null){
        this.db.close((err) => {
          this.db = null;
          if (err){
            reject(err);
          }
          else {
            resolve();
          }
        });
      }
      else {
        resolve();
      }
    });
  }

  this.getClient = async function() {
    return this.db;
  }


  this.releaseClient = async function(client) {
  }


  /*
   * Return an array of rows, or [] if the query returned no data
   */
  this.query = function(client, sql, params) {
    return new Promise((resolve,reject) => {
      client.all(sql, params, function(err, results) {
        if (err){
          reject(err);
        }
        else {
          resolve(results);
        }
      });
    });
  }


  /*
   * Return the number of rows affected by the query
   */
  this.exec = function(client, sql, params) {
    return new Promise((resolve,reject) => {
      // no arrow function otherwise we don't get 'lastID' and 'changes'
      client.run(sql, params, function(err) {
        if (err){
          reject(err);
        }
        else {
          resolve({changes:this.changes, lastID:this.lastID});
        }
      });
    });
  }


  this.executeTransaction = function(client, tx_isolation_level, fnExec, conn) {
    // const fnTx = transactionIsolationLevels[tx_isolation_level];
    // if (fnTx == null){
    //   throw `Unrecogned transaction isolation level [${tx_isolation_level}]!`;
    // }

    // return new Promise((resolve, reject) => {
    //   client[fnTx](() => {
    //     fnExec(conn)
    //       .then(res => resolve(res))
    //       .catch(err => reject(err));
    //   });
    // });


    return new Promise((resolve, reject) => {
      client.serialize(() => {
        let startTx;
        if (Object.values(transactionIsolationLevels).includes(tx_isolation_level)) {
          logger.trace("Begin transaction with [%s]", tx_isolation_level);
          //this.db.run(`BEGIN ${tx_isolation_level} TRANSACTION`);
          startTx = conn.exec;
        }
        else {
          startTx = async () => {};
        }

        startTx(`BEGIN ${tx_isolation_level} TRANSACTION`)
        .then(() => {
          fnExec(conn)
          .then(res => {
            logger.trace("Transaction successfully completed: ", res);
            conn.commit()
            .then(() => resolve(res))
            .catch(err => reject(err));
          })
          .catch(err => {
            logger.trace("Transaction failure: ", err);
            conn.rollback(err)
            .then(() => reject(err))
            .catch(() => reject(err));
          });
        })
        .catch(err => {
          reject(err);
        });
      });
    });
  }


  this.ensureMigrationsTable = function (migrationsTableName) {
    return new Promise((resolve, reject) => {
      this.db.serialize(() => {
        this.db.run(`CREATE TABLE IF NOT EXISTS ${migrationsTableName}(name VARCHAR(128) NOT NULL PRIMARY KEY ON CONFLICT ROLLBACK, updated_at INTEGER NOT NULL)`, [], function(err) {
          if (err){
            reject(err);
          }
          else {
            logger.debug("Migrations table checked OK.");
            resolve();
          }
        });
      });
    });
  }


  this.listExecutedMigrationNames = function(migrationsTableName) {
    return new Promise((resolve, reject) => {
      this.db.parallelize(() => {
        this.db.all(`SELECT name FROM ${migrationsTableName} ORDER BY name`, [], function(err,rows){
          if (err){
            reject(err);
          }
          else {
            resolve(rows.map(r => r.name));
          }
        });
      });
    });
  }


  this.logMigrationSuccessful = function(conn, migrationsTableName, migrationName) {
    return new Promise((resolve, reject) => {
      this.db.parallelize(() => {
        this.db.run(`INSERT INTO ${migrationsTableName}(name,updated_at) VALUES(?,?)`, [migrationName, this.timestamp()], function(err){
          if (err){
            reject(err);
          }
          else {
            resolve();
          }
        });
      });
    });
  }

  
  this.getMigrationTransactionIsolationLevel = function() {
    return transactionIsolationLevels.exc;
  }
}


Object.assign(Sqlite3.prototype, driverPrototype);
module.exports = Sqlite3;
