'use strict';
const TH = require('./test-helper');
const should = require('should');

describe('Migrations', () => {

  const withDriver = TH.beforeEach([["migs"]]);

  describe('ensureMigrationsTable', () => {
    it("should create a migration table when it does not exist", async () => {
      await withDriver(async (sqlite, client) => {
        // Ensure there is no 'migs' table
        try{
          await sqlite.query(client, "SELECT * FROM migs");
          should.fail("Should not get here!");
        }
        catch(err){
          err.errno.should.equal(1);
        }

        // Create the migs table
        await sqlite.ensureMigrationsTable('migs');

        // Query should now work
        const rows = await sqlite.query(client, "SELECT * FROM migs");
        rows.should.eql([]);
      });
    });

    it("should not create a migration table when one already exist", async () => {
      await withDriver(async (sqlite, client) => {
        // Create the migs table
        await sqlite.exec(client, "CREATE TABLE migs(name VARCHAR(128) NOT NULL, updated_at TIMESTAMP NOT NULL, PRIMARY KEY(name))");
        let rows = await sqlite.query(client, "SELECT * FROM migs");
        rows.should.eql([]);

        // Do not re-create the migs table
        await sqlite.ensureMigrationsTable('migs');

        // Query should still work
        rows = await sqlite.query(client, "SELECT * FROM migs");
        rows.should.eql([]);
      });
    });
  });


  describe('listExecutedMigrationNames', () => {
    it("should return an empty list when there are no completed migrations", async () => {
      await withDriver(async (sqlite, client) => {
        await sqlite.ensureMigrationsTable('migs');
        const names = await sqlite.listExecutedMigrationNames('migs');
        names.should.eql([]);
      });
    });

    it("should return the list of completed migrations", async () => {
      await withDriver(async (sqlite, client) => {
        await sqlite.ensureMigrationsTable('migs');

        const now = Date.now();
        await sqlite.exec(client, "INSERT INTO migs(name, updated_at) VALUES('001-init',?),('002-blah',?)", [sqlite.timestamp(now), sqlite.timestamp(now+10000)]);

        const names = await sqlite.listExecutedMigrationNames('migs');
        names.should.eql(['001-init', '002-blah']);
      });
    });
  });


  describe('logMigrationSuccessful', () => {
    it("should log migrations", async () => {
      await withDriver(async (sqlite, client) => {
        await sqlite.ensureMigrationsTable('migs');
        (await sqlite.query(client, "SELECT name FROM migs ORDER BY name")).should.eql([]);

        const conn = {
          exec: (sql, params) => sqlite.exec(client, sql, params)
        };
        await sqlite.logMigrationSuccessful(conn, 'migs', '1-mig');
        await sqlite.logMigrationSuccessful(conn, 'migs', '2-mig');
        const rows = await sqlite.query(client, "SELECT name FROM migs ORDER BY name");
        rows.should.eql([{name:'1-mig'},{name:'2-mig'}]);
      });
    });
  });


  it("should expose the transaction isolation level to be used during migrations", async () => {
    await withDriver(async (sqlite, client) => {
      sqlite.getMigrationTransactionIsolationLevel().should.equal('EXCLUSIVE');
    });
  });

});
