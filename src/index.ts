import { Database, Statement, Transaction, TransactionOptions } from 'squeamish';
import { containsClauses, filterClauses } from './util';
import * as uuid from 'uuid';
import * as Rx from '@reactivex/rxjs'; 
import * as Bluebird from 'bluebird';

function optOp(operator: string, value?: string | number): string {
  if (value && value.toString().trim().length > 0) {
    return `${operator} ${value}`;
  } else {
    return "";
  }
}

function logDatabase(db: Database) {
  for (let func of ['runAsync', 'execAsync', 'eachAsync', 'allAsync', 'getAsync']) {
    const oldF = (db as any)[func].bind(db);
    (db as any)[func] = function() {return oldF(...arguments).catch((err: any) => {
      if (err.cause) {
        err.cause.func = func;
        err.cause.args = arguments;
      }
      throw err;
    })};
  }
}

export interface DBCursor extends Rx.Observable<any> {
  limit(limit: number): DBCursor;
  take(limit: number): DBCursor;
  sort(orderObject: any): DBCursor;
}

export interface UpdateSpec {
  upsert?: boolean;
  multi?: boolean;
}

export class Store { 
  database: Database;
  path: string;
  pool: Set<Database>;
  logging = false;
  transaction?: Transaction;

  async open(path: string, logging = false, journalMode = "WAL") {
    this.path = path;
    this.database = new Database(path);
    await this.database.execAsync(`PRAGMA journal_mode=${journalMode};`);
    this.logging = logging;
    if (this.logging) {
      this.database.sqlite.on('trace', console.log);
      //logDatabase(this.database);
    }
    this.pool = new Set<Database>();
  }

  async getFromPool(): Promise<Database> {
    if (this.transaction) {
      throw new Error("Don't open a handle within a transaction!");
    }
    if (this.pool.size > 0 ){
      const db = this.pool.values().next().value;
      this.pool.delete(db);
      return db;
    } else {
      const db = new Database(this.path);
      if (this.logging) {
        db.sqlite.on('trace', console.log);
        //logDatabase(db);
      }
      return db;
    }
  }

  async withinTransaction(fn:(s: Store) => Promise<void>, to?: TransactionOptions) {
    const s = new Store();
    s.path = this.path;
    s.database = this.database;
    s.logging = this.logging;
    if (this.transaction) {
      s.transaction = await this.transaction.beginTransaction();
    } else {
      const db = await this.getFromPool();
      s.transaction = await db.beginTransaction(to);
    }

    await fn(s).then(() => {
      if (s.transaction) { return s.transaction.commit(); }
    }, (error) => {
      if (s.transaction) { return s.transaction.rollback().then(() => { throw error; });}
    });
  }

  async returnToPool(db: Database) {
    this.pool.add(db);
  }

  getCollection(name: string) {
    const c = new Collection(this, name);
    return c.initialize();
  }

  async close() {
    return this.database.closeAsync();
  }

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
          const q = new Query({subQueries: [[`"${table}".value IN (${qMarks})`, value['$in']]]}, this.name, this.arrayIndexes);
          q.otherTable = `INNER JOIN "${table}" ON "${table}"._id = "${this.name}"._id`;
          //console.log(q);
          return q;

        } else { //unindexed

          const identifier = 'foo'; //TODO 
          const tableFunc = `json_each(document, '$.${key}') AS ${identifier}`;

          const qMarks = "?, ".repeat(value['$in'].length -1) + "?";
          const q = new Query({subQueries: [[`"${identifier}".value IN (${qMarks})`, value['$in']]]}, this.name, this.arrayIndexes);
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
    if (!component) {return []}

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

export type CollectionInitializedStatus = "uninitialized" | "initializing" | "initialized";
export type IndexOptions = { unique: boolean };

export class Collection {
  store: Store;
  name: string;
  private initializedStatus: CollectionInitializedStatus;
  arrayIndexes: Map<string, string>;
  idField = "_id";
  private dbIdField = "_id";

  get transaction() {
    return this.store.transaction;
  }

  private getMainHandle() {
    return this.transaction || this.store.database;
  }

  private returnHandleToPool(db: Database) {
    this.store.returnToPool(db);
  }

  private queryFor(q: any, operator?: "AND" | "OR") {
    if (this.idField != this.dbIdField) {
      q[this.dbIdField] = q[this.idField];
      delete q[this.idField];
    }
    return new Query({queryObject: q}, this.name, this.arrayIndexes, operator);
  }

  insertMany(data: any[]): Promise<void> {
    return this.withinTransaction(async c => {
      await Promise.all(data.map(x => c.insert(x)));
    });
  }

  constructor(store: Store, name: string, status: CollectionInitializedStatus = "uninitialized") {
    this.store = store;
    this.name = name;
    this.arrayIndexes = new Map<string, string>();
    this.initializedStatus = status;
  }

  async withinTransaction(fn:(c: Collection) => Promise<void>, to?: TransactionOptions) {
    return this.store.withinTransaction(async s => {
     const c = new Collection(s, this.name, this.initializedStatus)
     //TODO this needs to be sourced from a metadata table;
     c.idField = this.idField;
     c.arrayIndexes = this.arrayIndexes;
     await fn(c);
    }, to);
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
      return this.getMainHandle()
        .runAsync(`CREATE TABLE IF NOT EXISTS "${this.name}" (_id TEXT PRIMARY KEY, document JSON);`)
        .then(() => {this.refreshArrayIndexes()})
        .then(() => {this.initializedStatus = "initialized";})
        .then(() => this);;
    }
    return Promise.resolve(this);
  }

  async ensureIndex(spec: {[key: string] : number}, options?: IndexOptions) {
    const name = this.name + "_" + Object.keys(spec).join("_");
    let uniq = "";
    if (options && options.unique) {
      uniq = " UNIQUE";
    }
    let sql = `CREATE${uniq} INDEX IF NOT EXISTS "${name}" on "${this.name}"(`;

    sql = sql + Object.keys(spec).map((key: string) => {
      const order = spec[key];
      return "json_extract(document, '$." + key + "') " + (order < 0 ? "DESC" : "ASC");
    }).join(", ");
    
    sql = sql + ")"
    await this.getMainHandle().runAsync(sql);
  }

  async ensureArrayIndex(key: string) {
    await this.withinTransaction(async collection => {
      await collection._ensureArrayIndex(key);
      this.arrayIndexes.set(key, collection.arrayIndexes.get(key));
    });
  }

  async _ensureArrayIndex(key: string) {
    if (this.arrayIndexes.has(key)) {
      return;
    }

    const t = this.transaction;
    if (!t) {throw new Error("Need a transaction to ensureArrayIndex")};

    const tableName = `${this.name}_${key}`;
    await t.runAsync(`CREATE TABLE "${tableName}" AS SELECT _id, json_each.* from "${this.name}", json_each(document, '$.${key}')`);

    await t.runAsync(`DROP TRIGGER IF EXISTS "${tableName}_insert_trigger"`);
    let sql = `CREATE TRIGGER "${tableName}_insert_trigger" AFTER INSERT ON "${this.name}"
    BEGIN
    INSERT INTO "${tableName}" SELECT NEW._id, json_each.* from json_each(NEW.document, '$.${key}');
    END;`;
    //console.log(sql);
    await t.runAsync(sql);

    await t.runAsync(`DROP TRIGGER IF EXISTS "${tableName}_update_trigger"`);
    sql = `CREATE TRIGGER "${tableName}_update_trigger" AFTER UPDATE ON "${this.name}"
    BEGIN
    DELETE FROM "${tableName}" WHERE _id = OLD._id;
    INSERT INTO "${tableName}" SELECT NEW._id, json_each.* from json_each(NEW.document, '$.${key}');
    END;`;
    //console.log(sql);
    await t.runAsync(sql);

    await t.runAsync(`DROP TRIGGER IF EXISTS "${tableName}_delete"`);
    sql = `CREATE TRIGGER "${tableName}_delete" AFTER DELETE ON "${this.name}"
    BEGIN
    DELETE FROM "${tableName}" WHERE _id = OLD._id;
    END;`;
    //console.log(sql);
    await t.runAsync(sql);

    //TODO update delete
    await t.runAsync('INSERT INTO collection_array_indexes VALUES (?, ?, ?)', [this.name, key, tableName]);
    this.arrayIndexes.set(key, tableName);
  }

  parseOrder(order: any) {
    return Object.keys(order).map( key => {
      if (key === this.idField) {
        return `_id ${parseInt(order[key]) < 0 ? 'DESC' : 'ASC'}`;
      } else {
        return `json_extract(document, '$.${key}') ${parseInt(order[key]) < 0 ? 'DESC' : 'ASC'}`;
      }
    }).join(", ");
  }

  findObservable(q?: any, limit?: number, order?: any): DBCursor {

    if (q && (q['$query'] || q['$order'])) {
      return this.findObservable(q['$query'], limit, q['$order']);
    }

    let observable: DBCursor  = this._find(q, limit, order) as any; 

    observable.take = (count: number) => {
      return this.findObservable(q, limit ? Math.min(count, limit) : count, order);
    }

    observable.limit = observable.take;

    observable.sort = (order: any) => {
      return this.findObservable(q, limit, order);
    };

    return observable; 
  }

  async find(q?: any, limit?: number): Promise<any[]> {
    return this.findObservable(q || {}, limit).toArray().toPromise();
  }

  private _find(q: any | null, limit?: number, order?: any): Rx.Observable<any> {
    const orderSQL = order ? this.parseOrder(order) : "";

    let docs:Rx.Observable<any>;
    const handle = this.transaction || this.getMainHandle();

    let populate = (doc: any) => {
      const parsed = JSON.parse(doc.document) || {};
      parsed[this.idField] = doc[this.dbIdField];
      return parsed;
    }

    if (q && Object.keys(q).length > 0) {
      const query = this.queryFor(q);
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT DISTINCT "${this.name}"._id, "${this.name}".document from "${this.name}" ${joins} ${optOp('WHERE', whereSQL)} ${optOp('ORDER BY', orderSQL)} ${optOp('LIMIT', limit)}`;
      docs = Rx.Observable.from(handle.select(sql, args));
    } else {
      docs = Rx.Observable.from(handle.select(`SELECT * from "${this.name}" ${optOp('ORDER BY', orderSQL)} ${optOp('LIMIT', limit)}`));
    }
    return docs.map(populate);
  }

  async findOne(q?: any): Promise<any> {
    const doc = await this.find(q, 1);
    if (doc.length > 0) {
      return doc[0];
    } else {
      return null;
    }
  }
  
  async insert(doc: any): Promise<any> {
    const db = this.getMainHandle();
    const id = doc[this.idField] || uuid.v4();
    doc[this.idField] = id;
    const json = JSON.stringify(doc, (key, value) => {
      if (key === this.idField) {
        return undefined;
      } else {
        return value;
      }
    });
    await db.runAsync(`INSERT INTO "${this.name}" VALUES (?, json(?));`, id, json);
    return doc;
  }

  async count(q: any): Promise<number> {
    const db = this.getMainHandle();
    if (q && Object.keys(q).length > 0) {
      const query = this.queryFor(q);
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT COUNT(*) from "${this.name}" ${joins} ${optOp('WHERE', whereSQL)}`;
      //console.log(sql, args);
      const doc:any = await db.getAsync(sql, args);
      return doc['COUNT(*)'];

    } else {
      const doc:any = await db.getAsync(`SELECT COUNT(*) from "${this.name}"`);
      return doc['COUNT(*)'];
    }
  }
  
  update(q: any, update: any, options?: UpdateSpec): Promise<void> {
    return this.withinTransaction(async collection => {
      await collection._update(q, update, options);
      return;
    });
  }
  
  private async _update(q: any, update: any, options?: UpdateSpec): Promise<void> {

    const query = this.queryFor(q);
    const t = this.transaction;
    if (!t) {throw new Error("update() should take place inside a transaction");}

    let whereSQL: string;

    let limit = ""
    if (!options || !options.multi)  {
      limit = "LIMIT 1";
    }

    // Unless the sqlite database is compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT
    // we can't set a LIMIT at the end of a statement without making the query more
    // complicated

    if (query.join().length > 0 || limit != "") {
      whereSQL = `_id IN (SELECT DISTINCT "${this.name}"._id FROM "${this.name}" ${query.join()} ${optOp('WHERE', query.toString())} ${limit} )`
    } else {
      whereSQL = `${query.toString()}`;
    }

    // emulate mongo behaviour
    // https://docs.mongodb.com/v3.2/reference/method/db.collection.update/#upsert-behavior
    if (options && options.upsert) {
      const matchingID = await t.getAsync(`SELECT _id FROM "${this.name}" ${optOp('WHERE', whereSQL)} ${(limit.length == 0) ? "LIMIT 1" : ""}`, query.values());
      if (!matchingID) {
        const id = update[this.idField] || q[this.idField];
        if (!containsClauses(update)) {
          if (!update[this.idField] && q[this.idField]) {
            update[this.idField] = q[this.idField];
          }
          await this.insert(update);
          return;
        } else {
          // From mongo docs:
          // Comparison operations from the <query> will not be included in the new document.
          let newDoc = filterClauses(q);
          newDoc = await this.insert(newDoc);
          await this._update(newDoc, update, {multi: false, upsert: false});
          return;
        }
      }
    }

    const keys = new Set<string>();
    if (update['$inc']) {
      for (let k of Object.keys(update['$inc'])) {
        const updateSQL = `UPDATE "${this.name}" SET document = json_set(document, '$.${k}', coalesce(json_extract(document, '$.${k}'), 0) + ?) ${optOp('WHERE', whereSQL)}`;

        const args = query.values();
        const val = update['$inc'][k];

        if (typeof val != 'number'){ 
          throw new Error("Can't increment by non-number type: " + k + " += " + val);
        }

        args.unshift(val);
        //console.log(updateSQL, args);
        await t.runAsync(updateSQL, args);
        keys.add(k);
      }
    }

    if (update['$set']) {
      for (let k of Object.keys(update['$set'])) {

        if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

        const updateSQL = `UPDATE "${this.name}" SET document = json_set(document, '$.${k}', ?) ${optOp('WHERE', whereSQL)}`;
        const args = query.values();
        let val = update['$set'][k];

        if (typeof val != 'number' && typeof val != 'string'){ 
          val = JSON.stringify(val);
        }

        args.unshift(val);
        //console.log(updateSQL, args);
        await t.runAsync(updateSQL, args);
        keys.add(k);
      }
    }

    if (update['$push']) {
      for (let k of Object.keys(update['$push'])) {
        if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

        // If nothing exists at that location, create an empty array
        const prepareSQL= `UPDATE "${this.name}" SET document = json_set(document, '$.${k}', json_array()) WHERE json_extract(document, '$.${k}') IS NULL ${optOp('AND', whereSQL)}`;
        const args:any[] = query.values() 
        await t.runAsync(prepareSQL, args);


        // I worked out that you can set the array element at (0-based) index n for n elements to append an element:
        const updateSQL = `UPDATE "${this.name}" SET document = json_set(document, '$.${k}[' || json_array_length(json_extract(document, '$.${k}')) || ']', ?) ${optOp('WHERE', whereSQL)}`;
        const val = update['$push'][k];

        if (typeof val == 'string' || typeof val == 'number'){ 
          args.unshift(val);
          /*        } else if (Array.isArray(val)){ 
                    for (let v of val) {
                    values.push(v);
                    }*/
      } else {
        args.unshift(JSON.stringify(val));
      }

          await t.runAsync(updateSQL, args);

          keys.add(k);
        }
      }

      if (update['$pop']) {
        for (let k of Object.keys(update['$pop'])) {
          if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }
          const val = update['$pop'][k];
          let updateSQL: string;
          if (val === 1) {
            updateSQL = `UPDATE "${this.name}" SET document = json_remove(document, '$.${k}[' || (json_array_length(json_extract(document, '$.${k}')) - 1) || ']') ${optOp('WHERE', whereSQL)}`;
          } else if (val === -1) {
            updateSQL = `UPDATE "${this.name}" SET document = json_remove(document, '$.${k}[0]') ${optOp('WHERE', whereSQL)}`;
          } else {
            throw new Error('Incorrect argument to $pop: ' + k + ' : ' + val);
          }
          const args:any[] = query.values();
          await t.runAsync(updateSQL, args);
          keys.add(k);
        }
      }

      if (!containsClauses(update)) {
        if (update[this.idField]) {
          delete update[this.idField];
        }
        const updateSQL = `UPDATE "${this.name}" SET document = json(?) ${optOp('WHERE', whereSQL)}`;
        const args = [JSON.stringify(update), ...query.values()];
        await t.runAsync(updateSQL, args);
      } else {
        if (keys.size == 0) {
          throw new Error("Couldn't create update for field: " + update);
        }
      }

    }
  }

