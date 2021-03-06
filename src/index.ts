import { Database, Statement, Handle, TransactionOptions } from 'squeamish';
import { containsClauses, filterClauses } from './util';
import * as uuid from 'uuid';
import * as Rx from 'rxjs'; 
import * as Bluebird from 'bluebird';
import { Query } from './query';

export { Query };

const metadataTableName = "dinky_metadata";

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
  count(): Rx.Observable<number>;
}

export interface UpdateSpec {
  upsert?: boolean;
  multi?: boolean;
}

export interface DeleteSpec {
  justOne: boolean;
}

export class Store { 
  path: string;
  pool: Set<Database>;
  logging = false;
  handle: Handle;

  _metadata?: Collection;

  async getMetadata(): Promise<Collection> {
    if (this._metadata) {
      return this._metadata;
    } else {
      const m = await this.getCollection(metadataTableName);
      this._metadata = m;
      return m;
    }
  }

  async open(path: string, logging = false, journalMode = "WAL") {
    this.path = path;
    const database = new Database(path);
    this.handle = database;
    await this.handle.execAsync(`PRAGMA journal_mode=${journalMode};`);
    this.logging = logging;
    if (this.logging) {
      database.sqlite.on('trace', console.log);
      logDatabase(database);
    }
    this._metadata = await this.getCollection(metadataTableName);
    this.pool = new Set<Database>();
  }

  async withinTransaction(fn:(s: Store) => Promise<void>, to?: TransactionOptions) {
    const s = new Store();
    s.path = this.path;
    s.logging = this.logging;
    const transaction = await this.handle.beginTransaction(to);
    s.handle = transaction;

    await fn(s).then(() => {
      return transaction.commit()
    }, (error) => {
      return transaction.rollback().then(() => { throw error; });
    });
  }

  async returnToPool(db: Database) {
    this.pool.add(db);
  }

  getCollection(name: string, idField?: string) {
    const c = new Collection(this, name, idField);
    return c.initialize();
  }

  async close() {
    if ((<Database>this.handle)) {
      return (<Database>this.handle).closeAsync();
    }
  }

}

type KeyValuePair = [string, any];

export type CollectionInitializedStatus = "uninitialized" | "initializing" | "initialized";
export type IndexOptions = { unique: boolean };

export class Collection {
  store: Store;
  name: string;
  private initializedStatus: CollectionInitializedStatus;
  arrayIndexes: Map<string, string>;
  idField: string;

  idFields(): {[id: string]: string} {
    const o:{[id: string]: string} = {};
    o[this.idField] = this.dbIdField;
    return o;
  }

  private dbIdField = "_id";

  private getMainHandle(): Handle {
    return this.store.handle;
  }

  private returnHandleToPool(db: Database) {
    this.store.returnToPool(db);
  }

  private queryFor(q: any, operator?: "AND" | "OR") {
    try {
      return new Query(q, this.name, this.arrayIndexes, this.idFields());
    } catch (error) {
      console.log("FAILED TO BUILD QUERY FOR ");
      console.dir(q, {depth: null});
      console.log(operator);
      throw error;
    }
  }

  insertMany(data: any[]): Promise<void> {
    return this.withinTransaction(async c => {
      await Promise.all(data.map(x => c.insert(x)));
    });
  }

  constructor(store: Store, name: string, idField?: string, status: CollectionInitializedStatus = "uninitialized") {
    this.store = store;
    this.name = name;
    if (idField) {
      this.idField = idField;
    }
    this.arrayIndexes = new Map<string, string>();
    this.initializedStatus = status;
  }

  async withinTransaction(fn:(c: Collection) => Promise<void>, to?: TransactionOptions) {
    return this.store.withinTransaction(async s => {
      const c = new Collection(s, this.name, this.idField, this.initializedStatus)
      //TODO this needs to be sourced from a metadata table;
      c.arrayIndexes = this.arrayIndexes;
      await fn(c);
    }, to);
  }

  async refreshArrayIndexes(arrayIndexes: any[]) {
    for (let index of arrayIndexes || []) {
      this.arrayIndexes.set(index.keypath, index.indexTable);
    }
  }

  async initialize(): Promise<Collection> {
    if (this.initializedStatus == "uninitialized") {
      this.initializedStatus = "initializing";
      const db:Handle = this.getMainHandle()
      await db.runAsync(`CREATE TABLE IF NOT EXISTS "${this.name}" (_id TEXT PRIMARY KEY, document JSON);`)

      if (this.name != metadataTableName) {
        const metadataCollection = await this.store.getMetadata();
        await metadataCollection.withinTransaction(async metadataCollection => {
          let metadata = await metadataCollection.findOne({_id: this.name});

          if (!metadata) {
            metadata = {_id: this.name, idField: this.dbIdField}
            await metadataCollection.insert(metadata);
          }

          if (this.idField) {
            await metadataCollection.update({_id: this.name}, {$set: {'idField': this.idField}}, {upsert: true});
          } else { 
            if (metadata && metadata.idField) {
              this.idField =  metadata.idField;
            } else {
              this.idField = this.dbIdField;
            }
          }

          if (metadata) {
            await this.refreshArrayIndexes(metadata.arrayIndexes);
          }
        }, {type: "IMMEDIATE"} );
      } else {
        this.idField = this.dbIdField;
      }
      this.initializedStatus = "initialized";
    }
    return this;
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

  async ensureArrayIndex(key: string, order:(1 | -1) = 1) {
    await this.withinTransaction(async collection => {
      await collection._ensureArrayIndex(key, order == 1 ? "ASC" : "DESC");
      this.arrayIndexes.set(key, collection.arrayIndexes.get(key));
    });
  }

  async _ensureArrayIndex(key: string, order: "ASC" | "DESC") {
    if (this.arrayIndexes.has(key)) {
      return;
    }

    const t = this.getMainHandle();

    const tableName = `${this.name}_${key}`;
    await t.runAsync(`CREATE TABLE IF NOT EXISTS "${tableName}" AS SELECT _id, json_each.* FROM "${this.name}", json_each(document, '$.${key}')`);
    await t.runAsync(`CREATE INDEX IF NOT EXISTS "${tableName}_${order}" ON "${tableName}" ("value" ${order})`);

    await t.runAsync(`DROP TRIGGER IF EXISTS "${tableName}_insert_trigger"`);
    let sql = `CREATE TRIGGER "${tableName}_insert_trigger" AFTER INSERT ON "${this.name}"
    BEGIN
    INSERT INTO "${tableName}" SELECT NEW._id, json_each.* FROM json_each(NEW.document, '$.${key}');
    END;`;
    //console.log(sql);
    await t.runAsync(sql);

    await t.runAsync(`DROP TRIGGER IF EXISTS "${tableName}_update_trigger"`);
    sql = `CREATE TRIGGER "${tableName}_update_trigger" AFTER UPDATE ON "${this.name}"
    BEGIN
    DELETE FROM "${tableName}" WHERE _id = OLD._id;
    INSERT INTO "${tableName}" SELECT NEW._id, json_each.* FROM json_each(NEW.document, '$.${key}');
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
    const metadata = await this.store.getMetadata();
    await metadata.update({_id: this.name}, {$push: {arrayIndexes: {keyPath: key, indexTable: tableName} } });
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

    observable.count = () => {
      return Rx.Observable.from(this.count(q));
    }

    return observable; 
  }

  async find(q?: any, limit?: number): Promise<any[]> {
    return this.findObservable(q || {}, limit).toArray().toPromise();
  }

  private _find(q: any | null, limit?: number, order?: any): Rx.Observable<any> {
    const orderSQL = order ? this.parseOrder(order) : "";

    let docs:Rx.Observable<any>;
    const handle = this.getMainHandle();

    let populate = (doc: any) => {
      const parsed = JSON.parse(doc.document) || {};
      parsed[this.idField] = doc[this.dbIdField];
      return parsed;
    }

    if (q && Object.keys(q).length > 0) {
      const query = this.queryFor(q);
      const whereSQL = query.sql;
      const args = query.values;
      let joins = query.join;
      const sql = `SELECT DISTINCT "${this.name}"._id, "${this.name}".document FROM "${this.name}" ${joins} ${optOp('WHERE', whereSQL)} ${optOp('ORDER BY', orderSQL)} ${optOp('LIMIT', limit)}`;
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

  strippedJSON(doc: any) {
    return JSON.stringify(doc, (key, value) => {
      if (key === this.idField) {
        return undefined;
      } else {
        return value;
      }
    });
  }
  
  async insert(doc: any): Promise<any> {
    const db = this.getMainHandle();
    const id = doc[this.idField] || uuid.v4();
    doc[this.idField] = id;
    await db.runAsync(`INSERT INTO "${this.name}" VALUES (?, json(?));`, id, this.strippedJSON(doc));
    return doc;
  }

  async count(q: any = {}): Promise<number> {
    const db = this.getMainHandle();
    if (q && Object.keys(q).length > 0) {
      const query = this.queryFor(q);
      const whereSQL = query.sql;
      const args = query.values;
      let joins = query.join;
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

  async save(doc: any): Promise<any> { 
    if (doc[this.idField]) {
      const id:any = {};
      id[this.idField] = doc[this.idField];
      await this.update(id, doc, {upsert: true});
      return this.findOne(id);
    } else {
      return this.insert(doc);
    }
  }

  update(q: any, update: any, options: UpdateSpec = {multi: false, upsert: false}): Promise<void> {
    return this.withinTransaction(async collection => {
      await collection._update(q, update, options);
      return;
    });
  }

  delete(q: any, options: DeleteSpec = {justOne: false}) {
    return this.withinTransaction(async collection => {
      await collection._update(q, "DELETE", options);
    });
  }

  private async _update(q: any, update: any, options: UpdateSpec | DeleteSpec): Promise<void> {

    const query = this.queryFor(q);
    const t = this.getMainHandle();
  
    let whereSQL: string;

    let limit = ""

    let operation: ("UPDATE" | "DELETE") = "UPDATE";

    //DELETE 
    if (typeof update === 'string' && update.toLowerCase() == 'delete') {
      operation = "DELETE";
    }

    if (operation == "UPDATE") {
      const updateOptions = options as UpdateSpec;
      if (!updateOptions.multi)  {
        limit = "LIMIT 1";
      }
    } else {
      const deleteOptions = options as DeleteSpec;
      limit = deleteOptions.justOne ? "LIMIT 1" : "";
    }

    // Unless the sqlite database is compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT
    // we can't set a LIMIT at the end of a statement without making the query more
    // complicated
    if (query.join.length > 0 || limit != "") {
      whereSQL = `_id IN (SELECT DISTINCT "${this.name}"._id FROM "${this.name}" ${query.join} ${optOp('WHERE', query.sql)} ${limit} )`
    } else {
      whereSQL = `${query.sql}`;
    }

    // emulate mongo behaviour
    // https://docs.mongodb.com/v3.2/reference/method/db.collection.update/#upsert-behavior
    if (operation === "UPDATE" && (options as UpdateSpec).upsert) {
      const matchingID = await t.getAsync(`SELECT _id FROM "${this.name}" ${optOp('WHERE', whereSQL)} ${(limit.length == 0) ? "LIMIT 1" : ""}`, query.values);
      if (!matchingID) {
        const id = update[this.idField] || q[this.idField];
        if (!containsClauses(update)) {
          if (!update[this.idField] && id) {
            update[this.idField] = id;
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

    if (operation == "DELETE") {
      const args = query.values;
      const updateSQL = `DELETE FROM "${this.name}" ${optOp('WHERE', whereSQL)}`;
      await t.runAsync(updateSQL, args);
      return;
    }

    //operation == "UPDATE"
    const keys = new Set<string>();
    if (update['$inc']) {
      for (let k of Object.keys(update['$inc'])) {
        const updateSQL = `UPDATE "${this.name}" SET document = json_set(document, '$.${k}', coalesce(json_extract(document, '$.${k}'), 0) + ?) ${optOp('WHERE', whereSQL)}`;

        const args = query.values;
        const val = update['$inc'][k];

        if (typeof val != 'number'){ 
          throw new Error("Can't increment by non-number type: " + k + " += " + val);
        }

        args.unshift(val);
        await t.runAsync(updateSQL, args);
        keys.add(k);
      }
    }

    if (update['$set']) {
      for (let k of Object.keys(update['$set'])) {

        if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

        const updateSQL = `UPDATE "${this.name}" SET document = json_set(document, '$.${k}', ?) ${optOp('WHERE', whereSQL)}`;
        const args = query.values;
        let val = update['$set'][k];

        if (typeof val != 'number' && typeof val != 'string' && typeof val != 'boolean'){ 
          val = JSON.stringify(val);
        }

        args.unshift(val);
        //console.log(updateSQL, args);
        await t.runAsync(updateSQL, args);
        keys.add(k);
      }
    }

    if (update['$addToSet']) {
      for (let k of Object.keys(update['$addToSet'])) {
        if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

        const myQ = {...q};
        myQ[k] = {'$nin': [update['$addToSet'][k]]};
        const updateSpec: any = {};
        updateSpec['$push'] = {};
        updateSpec['$push'][k] = update['$addToSet'][k];

        await this._update(myQ, updateSpec, options);

        keys.add(k);
      }
    }

    if (update['$push']) {
      for (let k of Object.keys(update['$push'])) {
        if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

        // If nothing exists at that location, create an empty array
        const prepareSQL= `UPDATE "${this.name}" SET document = json_set(document, '$.${k}', json_array()) WHERE json_extract(document, '$.${k}') IS NULL ${optOp('AND', whereSQL)}`;
        const args:any[] = query.values 
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
        const args:any[] = query.values;
        await t.runAsync(updateSQL, args);
        keys.add(k);
      }
    }

    if (!containsClauses(update)) {
      const updateSQL = `UPDATE "${this.name}" SET document = json(?) ${optOp('WHERE', whereSQL)}`;
      const args = [this.strippedJSON(update), ...query.values];
      await t.runAsync(updateSQL, args);
    } else {
      if (keys.size == 0) {
        throw new Error("Couldn't create update for field: " + update);
      }
    }

  }
}

