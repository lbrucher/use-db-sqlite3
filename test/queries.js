'use strict';
const TH = require('./test-helper');
const Sqlite3 = require('../index');
const should = require('should');

describe('Queries', () => {
  const tables = [["test", "id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK AUTOINCREMENT", "name TEXT NOT NULL", "zip INTEGER", "city TEXT"]];

  const withDriver = TH.beforeEach(tables);


  it("should execute a query", async () => {
    await withDriver(async (sqlite, client) => {
      (await sqlite.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.eql({changes:1, lastID:1});
      (await sqlite.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.eql({changes:1, lastID:2});
      (await sqlite.exec(client, "INSERT INTO test(name,zip,city) VALUES('grace', 1390, 'Grez')")).should.eql({changes:1, lastID:3});

      (await sqlite.exec(client, "UPDATE test SET city='Lathuy' WHERE name='mary'")).should.eql({changes:1, lastID:3});
      (await sqlite.exec(client, "UPDATE test SET city='Lathuy' WHERE name='gary'")).should.eql({changes:0, lastID:3});

      const rows = await sqlite.query(client, "SELECT * FROM test WHERE zip=1390");
      rows.should.eql([
        {id:1, name:'john', zip:1390, city:'Nethen'},
        {id:3, name:'grace', zip:1390, city:'Grez'}
      ]);
    });
  });

  it("should execute a query returning no data", async () => {
    await withDriver(async (sqlite, client) => {
      (await sqlite.exec(client, "INSERT INTO test(name,zip,city) VALUES('john', 1390, 'Nethen')")).should.eql({changes:1, lastID:1});
      (await sqlite.exec(client, "INSERT INTO test(name,zip,city) VALUES('mary', 1300, 'Jodoigne')")).should.eql({changes:1, lastID:2});
 
      const rows = await sqlite.query(client, "SELECT * FROM test WHERE zip=1200");
      rows.should.eql([]);
    });
  });

});
