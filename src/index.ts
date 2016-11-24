import { SQLite, Database, Statement } from 'squeamish';
import * as uuid from 'uuid';

export class Store { 
  database: Database;
  path: string;
  pool: Set<Database>;
  logging = false;

  async open(path: string, logging = false) {
    this.path = path;
    this.database = await SQLite.open(path);
    this.logging = logging;
    if (this.logging) {
      this.database.on('trace', console.log);
    }
    this.pool = new Set<Database>();
  }

  async getFromPool(): Promise<Database> {
    if (this.pool.size > 0 ){
      const db = this.pool.values().next().value;
      this.pool.delete(db);
      return db;
    } else {
      const db = await SQLite.open(this.path);
      if (this.logging) {
        db.on('trace', console.log);
      }
      return db;
    }
  }

  async returnToPool(db: Database) {
    this.pool.add(db);
  }

  async getCollection(name: string) {
    const c = new Collection(this, name);
    await c.initialize();
    return c;
  }

  async close() {
    return this.database.closeAsync();
  }

}

// Not supported yet
// upsert is hard
// multi requires SQLITE_ENABLE_UPDATE_DELETE_LIMIT and my SQLite3 doesn't seem to have it.
export interface UpdateSpec {
  upsert?: boolean;
  multi?: boolean;
}

type KeyValuePair = [string, any];
type SubQueryContainer = {subQueries: (Query | KeyValuePair)[]};
type QueryObjectContainer = {queryObject: any};

class Query {
  subQueries: (Query | KeyValuePair)[];
  operator: "AND" | "OR";
  otherTable?: string;
  arrayIndexes: Map<string, string>;
  name: string;


  constructor(q:SubQueryContainer | QueryObjectContainer, tableName: string, arrayIndexes: Map<string, string>, operator?: "AND" | "OR") {
    this.arrayIndexes = arrayIndexes;
    this.name = tableName;
    if ((<SubQueryContainer>q).subQueries) {
      this.subQueries = (<SubQueryContainer>q).subQueries;
      this.operator = operator || "AND";
    } else {
      this.subQueries = this.parseComponent((<QueryObjectContainer>q).queryObject);
      this.operator = operator || "AND";
    }
  }

  parseKeyValue(key: string, value: any) : ([string, any] | Query) {
    if (key.startsWith('$')) {
      if (key.toLowerCase() == '$and') {
        return new Query({queryObject: value}, this.name, this.arrayIndexes,  "AND");
      } else if (key.toLowerCase() == '$or') {
        return new Query({queryObject: value}, this.name, this.arrayIndexes, "OR");
      } else if (key.toLowerCase() == '$not') {
        return new Query({queryObject: value}, this.name, this.arrayIndexes);
      } else {
        throw new Error("Unsupported query term " + key);
      }
    } else {
      if (typeof value == 'string' || typeof value == 'number') {
        return [`json_extract(document, '$.${key}') IS ?`, value];
      } else if (Array.isArray(value['$in'])) {
        //console.log(this.arrayIndexes, key);
        if (this.arrayIndexes.has(key)) { //indexed

          const table = this.arrayIndexes.get(key);
          const qMarks = "?, ".repeat(value['$in'].length -1) + "?";
          const q = new Query({subQueries: [[`${table}.value IN (${qMarks})`, value['$in']]]}, this.name, this.arrayIndexes);
          q.otherTable = `INNER JOIN ${table} ON ${table}._id = ${this.name}._id`;
          //console.log(q);
          return q;

        } else { //unindexed

          const identifier = 'foo'; //TODO 
          const tableFunc = `json_each(document, '$.${key}') AS ${identifier}`;

          const qMarks = "?, ".repeat(value['$in'].length -1) + "?";
          const q = new Query({subQueries: [[`${identifier}.value IN (${qMarks})`, value['$in']]]}, this.name, this.arrayIndexes);
          q.otherTable = ", " + tableFunc;
          //console.log(q);
          return q;

        }
      } else if (value['$not'] || value['$NOT']) {
        const finalVal = value['$not'] || value['$NOT'];
        if (typeof finalVal == 'string' || typeof finalVal== 'number') {
          return [`json_extract(document, '$.${key}') IS NOT ?`, finalVal];
        } else {
          throw new Error("Unsupported query " + value);
        }
      } else {
        throw new Error("Unsupported query " + value);
      }
    }
  }

  parseComponent(component: any) : ([string, any] | Query)[] {
    if (Array.isArray(component)) {
      return component.map(x => new Query({queryObject: x}, this.name, this.arrayIndexes));
    }

    const queries = Object
      .keys(component)
      .filter(x => x != '_id')
      .map( (key) => {
        return this.parseKeyValue(key, component[key]);
      });
    if (component['_id']) {
      queries.push([`_id IS ?`, component['_id']]);
    }
    return queries;
  }


  toString(): string {
    return this.subQueries.map( q => {
      if (q instanceof Query) {
        return "(" + q.toString() + ")";
      } else {
        return q[0];
      }
    }).join(" " + this.operator + " ");
  }

  values(): any[] {
    return this.subQueries.map( q => {
      if (q instanceof Query) {
        return q.values();
      } else {
        return q[1];
      }
    }).reduce((x,y) => x.concat(y), []);
  }

  join(): string[] {
    let joins: string[] = [];
    if (this.otherTable) {
      joins.push(this.otherTable);
    }
    this.subQueries.forEach( q => {
      if (q instanceof Query) {
        joins = joins.concat(q.join());
      }
    });
    return joins;
  }
}

export type Transaction = {handle: Database, name?: string};

export class Collection {
  store: Store;
  name: string;
  initializedStatus: "uninitialized" | "initializing" | "initialized";
  arrayIndexes: Map<string, string>;

  private async getHandleFromPool(): Promise<Database> {
    return this.store.getFromPool();
  }

  /**
    Please don't use this handle for anything requiring transactions.
    Other things could run async and there's nothing to stop them running
    random statements inside your transaction.
    */
  private getMainHandle() {
    return this.store.database;
  }

  private returnHandleToPool(db: Database) {
    this.store.returnToPool(db);
  }

  private queryFor(q: any, operator?: "AND" | "OR") {
    return new Query({queryObject: q}, this.name, this.arrayIndexes, operator);
  }

  async beginTransaction(outerTransaction?: Transaction): Promise<Transaction> {
    if (outerTransaction) {
      const db = outerTransaction.handle;
      const transactionName = uuid.v4();
      await db.execAsync(`SAVEPOINT '${transactionName}'`);
      return {handle: db, name: transactionName};
    } else {
      const db = await this.getHandleFromPool();
      await db.execAsync(`BEGIN TRANSACTION`);
      return {handle: db};
    }
  }

  async commit(transaction: Transaction) {
    if (transaction.name) {
      await transaction.handle.execAsync(`RELEASE '${transaction.name}'`);
    } else {
      await transaction.handle.execAsync(`COMMIT`);
      this.returnHandleToPool(transaction.handle);
    }
  }
  
  async rollback(transaction: Transaction) {
    if (transaction.name) {
      await transaction.handle.execAsync(`ROLLBACK TO '${transaction.name}'`);
    } else {
      await transaction.handle.execAsync(`ROLLBACK`);
      this.returnHandleToPool(transaction.handle);
    }
  }

  async insertMany(data: any[], outerTransaction?: Transaction) {
    const t = await this.beginTransaction(outerTransaction);
    await Promise.all(data.map(x => this.insert(x, t)));
    await this.commit(t);
  }

  constructor(store: Store, name: string) {
    this.store = store;
    this.name = name;
    this.arrayIndexes = new Map<string, string>();
    this.initializedStatus = "uninitialized";
  }

  async refreshArrayIndexes() {
    await this.getMainHandle().runAsync(`CREATE TABLE IF NOT EXISTS collection_array_indexes (sourceTable TEXT, keypath TEXT, indexTable TEXT);`);
    const arrayIndexes = await this.getMainHandle().allAsync("SELECT * from collection_array_indexes WHERE sourceTable = ?", this.name);
    for (let index of arrayIndexes) {
      this.arrayIndexes.set(index.keypath, index.indexTable);
    }
  }

  async initialize() {
    if (this.initializedStatus == "uninitialized") {
      this.initializedStatus = "initializing";
      await this.getMainHandle().runAsync(`CREATE TABLE IF NOT EXISTS ${this.name} (_id TEXT PRIMARY KEY, document JSON);`);
      await this.refreshArrayIndexes();
      this.initializedStatus = "initialized";
    }
  }

  async ensureIndex(spec: {[key: string] : number}) {
    const name = this.name + "_" + Object.keys(spec).join("_");

    let sql = `CREATE INDEX IF NOT EXISTS ${name} on ${this.name}(`;

    sql = sql + Object.keys(spec).map((key: string) => {
      const order:number = spec[key];
      return "json_extract(document, '$." + key + "') " + (order ? "DESC" : "ASC");
    }).join(", ");
    
    sql = sql + ");"
    await this.getMainHandle().runAsync(sql);
  }

  async ensureArrayIndex(key: string, outerTransaction?: Transaction) {
    if (this.arrayIndexes.has(key)) {
      return;
    }
    const t = await this.beginTransaction(outerTransaction);

    try {
     const tableName = `${this.name}_${key}`;
      await t.handle.runAsync(`CREATE TABLE '${tableName}' AS SELECT _id, json_each.* from ${this.name}, json_each(document, '$.${key}')`);

      await t.handle.runAsync(`DROP TRIGGER IF EXISTS ${tableName}_insert_trigger`);
      let sql = `CREATE TRIGGER '${tableName}_insert_trigger' AFTER INSERT ON ${this.name}
      BEGIN
      INSERT INTO ${tableName} SELECT NEW._id, json_each.* from json_each(NEW.document, '$.${key}'), ${this.name};
      END;`;
      await t.handle.runAsync(sql);

      await t.handle.runAsync(`DROP TRIGGER IF EXISTS ${tableName}_update_trigger`);
      sql = `CREATE TRIGGER '${tableName}_update_trigger' AFTER UPDATE ON ${this.name}
      BEGIN
      DELETE FROM ${tableName} WHERE _id = OLD._id;
      INSERT INTO ${tableName} SELECT NEW._id, json_each.* from json_each(NEW.document, '$.${key}'), ${this.name};
      END;`;
      await t.handle.runAsync(sql);

      await t.handle.runAsync(`DROP TRIGGER IF EXISTS ${tableName}_delete`);
      sql = `CREATE TRIGGER '${tableName}_delete' AFTER UPDATE ON ${this.name}
      BEGIN
      DELETE FROM ${tableName} WHERE _id = OLD._id;
      END;`;
      await t.handle.runAsync(sql);

      //TODO update delete
      await t.handle.runAsync('INSERT INTO collection_array_indexes VALUES (?, ?, ?)', [this.name, key, tableName]);
      await this.commit(t);
      this.arrayIndexes.set(key, tableName);
    } catch (error) {
      this.rollback(t);
      throw error;
    }
  }

  

  async find(q?: any, limit?: number, transaction?: Transaction): Promise<any[]> {
    let docs:any[];
    const handle = transaction ? transaction.handle : this.getMainHandle();

    if (q && Object.keys(q).length > 0) {
      const query = this.queryFor(q);
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT DISTINCT ${this.name}._id, ${this.name}.document from ${this.name} ${joins} WHERE ${whereSQL} ${(limit == undefined) ? "" : "LIMIT " + limit}`;
      //console.log(sql, args);
      docs = await handle.allAsync(sql, args);
    } else {
      docs = await handle.allAsync(`SELECT * from ${this.name}`);
    }

    return docs.map(doc => {
      const parsed = JSON.parse(doc.document);
      parsed._id = doc._id;
      return parsed;
    });
  }

  async findOne(q?: any): Promise<any> {
    const doc = await this.find(q, 1);
    if (doc.length > 0) {
      return doc[0];
    } else {
      return null;
    }
  }

  async insert(doc: any, transaction?: Transaction) {
    const db = transaction ? transaction.handle : this.getMainHandle();

    if (!doc._id) {
      doc._id = uuid.v4();
    }

    await db.runAsync(`INSERT INTO ${this.name} VALUES (?, ?);`, doc._id, JSON.stringify(doc, (key, value) => {
      if (key === '_id') {
        return undefined;
      } else {
        return value;
      }
    }));
  }

  async count(q: any, transaction?: Transaction): Promise<number> {
    const db = transaction ? transaction.handle : this.getMainHandle();
    if (q && Object.keys(q).length > 0) {
      const query = this.queryFor(q);
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT COUNT(*) from ${this.name} ${joins} WHERE ${whereSQL}`;
      //console.log(sql, args);
      const doc:any = await db.getAsync(sql, args);
      return doc['COUNT(*)'];

    } else {
      const doc:any = await db.getAsync(`SELECT COUNT(*) from ${this.name}`);
      return doc['COUNT(*)'];
    }
  }
  
  async update(q: any, update: any, options?: UpdateSpec, outerTransaction?: Transaction) {

    const query = this.queryFor(q);
    const t = await this.beginTransaction(outerTransaction);

    let whereSQL: string;

    let limit = ""
    if (!options || !options.multi)  {
      limit = "LIMIT 1";
    }

    // Unless the sqlite database is compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT
    // we can't set a LIMIT at the end of a statement without making the query more
    // complicated

    if (query.join().length > 0 || limit != "") {
      whereSQL = `_id IN (SELECT DISTINCT \`${this.name}\`.\`_id\` FROM ${this.name} ${query.join()} WHERE ${query.toString()} ${limit} )`
    } else {
      whereSQL = `${query.toString()}`;
    }

    //emulate mongo behaviour
    //https://docs.mongodb.com/v3.2/reference/method/db.collection.update/#upsert-behavior
    if (options && options.upsert) {
      const matchingID = await t.handle.execAsync(`SELECT ${whereSQL} LIMIT 1`);
      if (!matchingID) {
        const id = update._id || q._id;
        if (!containsClauses(update)) {
          if (!update._id && q._id) {
            update._id = q._id;
          }
          await this.insert(update, t);
          return this.commit(t);
        } else {
          //From mongo docs:
          // Comparison operations from the <query> will not be included in the new document.
          const newDoc = filterClauses(q);
          this.insert(newDoc, t);
          await this.update(newDoc, update, undefined, t);
          return this.commit(t);
        }
      }
    }

    try {
      const keys = new Set<string>();
      if (update['$inc']) {
        for (let k of Object.keys(update['$inc'])) {
          const updateSQL = `UPDATE ${this.name} SET document = json_set(document, '$.${k}', coalesce(json_extract(document, '$.${k}'), 0) + ?) WHERE ${whereSQL}`;

          const args = query.values();
          const val = update['$inc'][k];

          if (typeof val != 'number'){ 
            throw new Error("Can't increment by non-number type: " + k + " += " + val);
          }

          args.unshift(val);
          //console.log(updateSQL, args);
          await t.handle.runAsync(updateSQL, args);
          keys.add(k);
        }
      }

      if (update['$set']) {
        for (let k of Object.keys(update['$set'])) {

          if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

          const updateSQL = `UPDATE ${this.name} SET document = json_set(document, '$.${k}', ?) WHERE ${whereSQL}`;
          const args = query.values();
          let val = update['$set'][k];

          if (typeof val != 'number' && typeof val != 'string'){ 
            val = JSON.stringify(val);
          }

          args.unshift(val);
          //console.log(updateSQL, args);
          await t.handle.runAsync(updateSQL, args);
          keys.add(k);
        }
      }

      if (update['$push']) {
        for (let k of Object.keys(update['$push'])) {
          if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }
          // I worked out that you can set the array element at (0-based) index n for n elements to append an element:

         //UPDATE people SET document = json_set(document, '$.hobbies[' || json_array_length(json_extract(document, '$.hobbies')) || ']', "Skateboarding")  WHERE json_extract(document, "$.firstname") = "Bart";
          const updateSQL = `UPDATE ${this.name} SET document = json_set(document, '$.${k}[' || json_array_length(json_extract(document, '$.${k}')) || ']', ?) WHERE ${whereSQL}`;
          const val = update['$push'][k];
          const args:any[] = query.values() 

          if (typeof val == 'string' || typeof val == 'number'){ 
            args.unshift(val);
            /*        } else if (Array.isArray(val)){ 
                      for (let v of val) {
                      values.push(v);
                      }*/
          } else {
            args.unshift(JSON.stringify(val));
          }

          await t.handle.runAsync(updateSQL, args);

          keys.add(k);
        }
      }

      if (update['$pop']) {
        for (let k of Object.keys(update['$pop'])) {
          if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }
          const val = update['$pop'][k];
          let updateSQL: string;
          if (val === 1) {
            updateSQL = `UPDATE ${this.name} SET document = json_remove(document, '$.${k}[' || (json_array_length(json_extract(document, '$.${k}')) - 1) || ']') WHERE ${whereSQL}`;
          } else if (val === -1) {
            updateSQL = `UPDATE ${this.name} SET document = json_remove(document, '$.${k}[0]') WHERE ${whereSQL}`;
          } else {
            throw new Error('Incorrect argument to $pop: ' + k + ' : ' + val);
          }
          const args:any[] = query.values();
          await t.handle.runAsync(updateSQL, args);
          keys.add(k);
        }
      }
    } catch (error) {
      await this.rollback(t);
      throw error;
    }

 //   const limit = (spec || {}).multi ? '' : 'LIMIT 1'
    await this.commit(t);
    return;
  }
}



function containsClauses(obj: any): boolean {
  if (obj === null || typeof obj !== "object") {
    return false;
  }

  if (Array.isArray(obj)) { 
    return obj.map(filterClauses).indexOf(true) != -1;
  }

  for (let key in Object.keys(obj)) {
    if (key.startsWith('$')) {
      return true;
    }
  }
  return false;
}
/**
 Remove $push, $pop, $in etc.
 */
function filterClauses(obj: any): any {
  const copy:any = {};

  if (obj === null || typeof obj !== "object") {
    return obj;
  }

  if (Array.isArray(obj)){ 
    return obj.map(filterClauses);
  }

  for (let key in Object.keys(obj)) {
    if (!key.startsWith('$')) {
      copy[key] = filterClauses(obj[key]);
    }
  }
}
