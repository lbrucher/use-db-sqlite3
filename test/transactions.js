'use strict';
const TH = require('./test-helper');
const Sqlite3 = require('../index');
const should = require('should');

describe('Transactions', () => {
  const tables = [
    ["address", "id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK AUTOINCREMENT", "street TEXT NOT NULL", "postcode INTEGER NOT NULL", "city TEXT NOT NULL"],
    ["user",    "id INTEGER PRIMARY KEY ON CONFLICT ROLLBACK AUTOINCREMENT", "name TEXT NOT NULL UNIQUE", "address_id INT REFERENCES address(id) ON DELETE CASCADE"]
  ];

  const withDriver = TH.beforeEach(tables);


  it("should commit a transaction", async () => {
    await withDriver(async (sqlite, client, conn) => {
      await sqlite.executeTransaction(client, sqlite.txIsolationLevels.def, async (conn) => {
        (await sqlite.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.eql({changes:1, lastID:1});
        (await sqlite.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Purple avenue', 1300, 'Jodoigne'),('Green road', 1390, 'Grez')")).should.eql({changes:2, lastID:3});
        (await sqlite.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 2),('Mary', 3)")).should.eql({changes:2, lastID:2});
  
        let rows = await sqlite.query(client, "SELECT * FROM address WHERE postcode=1390");
        rows.should.eql([
          {id:1, street:'Red avenue', postcode:1390, city:'Nethen'},
          {id:3, street:'Green road', postcode:1390, city:'Grez'}
        ]);
  
        rows = await sqlite.query(client, "SELECT * FROM user ORDER BY name");
        rows.should.eql([
          {id:1, name:'John', address_id:2},
          {id:2, name:'Mary', address_id:3}
        ]);
      }, conn);
    });

    // Now check that we can still find those records in the DB
    await withDriver(async (sqlite, client) => {
      const addresses = await sqlite.query(client, "SELECT * FROM address");
      const users = await sqlite.query(client, "SELECT name FROM user ORDER BY name");

      addresses.length.should.equal(3);
      users.should.eql([{name:'John'},{name:'Mary'}]);
    });
  });


  it("should fail a transaction", async () => {
    // Create a user and its adress
    await withDriver(async (sqlite, client, conn) => {
      await sqlite.executeTransaction(client, sqlite.txIsolationLevels.def, async (conn) => {
        (await sqlite.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.eql({changes:1, lastID:1});
        (await sqlite.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 1)")).should.eql({changes:1, lastID:1});
      }, conn);
    });

    // Create a second user with the same name as the first user
    try {
      await withDriver(async (sqlite, client, conn) => {
        await sqlite.executeTransaction(client, sqlite.txIsolationLevels.def, async (conn) => {
          (await sqlite.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Green avenue', 1300, 'Jodoigne')")).should.eql({changes:1, lastID:2});
          await sqlite.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 2)");
          await conn.commit();
          should.fail("Should not get here!");
        }, conn);
      });
    }
    catch(err){
      err.errno.should.equal(19);
    }

    // Now verify that the second address and user were in effect not added to the DB
    await withDriver(async (sqlite, client) => {
      const addresses = await sqlite.query(client, "SELECT * FROM address");
      const users = await sqlite.query(client, "SELECT * FROM user ORDER BY name");

      addresses.should.eql([{id:1, street:'Red avenue', postcode:1390, city:'Nethen'}]);
      users.should.eql([{id:1, name:'John', address_id:1}]);
    });
  });


  it("should rollback a transaction", async () => {
    // Create a user and its adress and then rollback instead of commit the transaction
    await withDriver(async (sqlite, client, conn) => {
      await sqlite.executeTransaction(client, sqlite.txIsolationLevels.def, async (conn) => {
        (await sqlite.exec(client, "INSERT INTO address(street,postcode,city) VALUES('Red avenue', 1390, 'Nethen')")).should.eql({changes:1, lastID:1});
        (await sqlite.exec(client, "INSERT INTO user(name,address_id) VALUES('John', 1)")).should.eql({changes:1, lastID:1});
        await conn.rollback();
      }, conn);
    });

    // Now verify that our DB is still empty
    await withDriver(async (sqlite, client) => {
      const addresses = await sqlite.query(client, "SELECT * FROM address");
      const users = await sqlite.query(client, "SELECT * FROM user ORDER BY name");

      addresses.should.eql([]);
      users.should.eql([]);
    });
  });
});
