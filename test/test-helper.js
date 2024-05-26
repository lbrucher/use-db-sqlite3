'use strict';
const Sqlite3 = require('../index');
const should = require('should');


exports.beforeEach = function(tables) {
  let sqlite;

  beforeEach(async () => {
    sqlite = new Sqlite3({filename: ":memory:"});
    await sqlite.initialize({logger:exports.noopLogger});

    // Drop the test table if one exists
    await exports.dropCreateTables(sqlite, tables);
  });

  function MakeConnection(sqlite, client) {
    this._ended = false;
    this.exec = async (sql,params) => {
      return await sqlite.exec(client, sql, params);
    }

    this.commit = async () => {
      if (this._ended){
        return;
      }
      await sqlite.exec(client, "COMMIT");
      this._ended = true;
    }

    this.rollback = async (err) => {
      if (this._ended){
        return;
      }
      await sqlite.exec(client, "ROLLBACK");
      this._ended = true;
    }
  }

  const withDriver = async (fnExec) => {
    let client;
    try {
      client = await sqlite.getClient();
      await fnExec(sqlite, client, new MakeConnection(sqlite,client));
    }
    finally {
      await sqlite.releaseClient(client);
    }
  };

  return withDriver;
}


exports.dropCreateTables = async function(sqlite, tables) {
  let client;
  try {
    client = await sqlite.getClient();

    // Drop all tables first
    // This needs to be done in reverse order compared to creating them
    const invTables = tables.slice(0);   // reverse() changes the array in-place so we slice(0) first
    invTables.reverse();
    for(const table of invTables) {
      try {
        await sqlite.exec(client, `DROP TABLE ${table[0]}`);
      }
      catch(err){
        err.errno.should.equal(1);
      }
    }

    // Then recreate all tables
    for(const table of tables) {
      const tableName = table[0];
      const tableFields = table.slice(1);
      if (tableFields.length > 0){
        try {
          await sqlite.exec(client, `CREATE TABLE ${tableName}(${tableFields.join(', ')})`);
        }
        catch(err){
          should.fail(err);
        }
      }
    }
  }
  finally {
    await sqlite.releaseClient(client);
  }
}


// exports.createDriver = async function(opts, fnExec) {
//   const sqlite = new Sqlite3(opts);
//   await sqlite.initialize({logger:exports.noopLogger});
//   let client;
//   try {
//     client = await sqlite.getClient();
//     await fnExec(sqlite, client);
//   }
//   finally {
//     await sqlite.releaseClient(client);
//     await sqlite.shutdown();
//   }
// }

exports.noopLogger = {
  trace: () => {},
  debug: () => {},
  info:  () => {},
  warn:  () => {},
  error: () => {}
}
