import { SQLite, Database, Statement } from 'squeamish';
import * as uuid from 'uuid';

export class Store { 
  database: Database;
  async open(path: string) {
    this.database = await SQLite.open(path);
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



class Query {
  subQueries: (Query | [string, any])[];
  operator: "AND" | "OR"
  otherTable?: string

  constructor(subQueries: (Query | [string, any])[], operator?: "AND" | "OR") {
    this.subQueries = subQueries;
    this.operator = operator || "AND";
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

export class Collection {
  store: Store;
  name: string;
  initializedStatus: "uninitialized" | "initializing" | "initialized";
  arrayIndexes: Map<string, string>;

  private async beginTransaction() {
    const db = this.store.database;
    await db.execAsync('BEGIN TRANSACTION');
    return db;
  }

  private async commit(db: Database) {
    await db.execAsync('COMMIT');
  }
  
  private async rollback(db: Database) {
    await db.execAsync('ROLLBACK');
  }

  async insertMany(data: any[]) {
    await this.store.database.runAsync('BEGIN TRANSACTION');
    await Promise.all(data.map(x => this.insert(x)));
    await this.store.database.runAsync('COMMIT');
  }

  constructor(store: Store, name: string) {
    this.store = store;
    this.name = name;
    this.arrayIndexes = new Map<string, string>();
    this.initializedStatus = "uninitialized";
  }

  async refreshArrayIndexes() {
    await this.store.database.runAsync(`CREATE TABLE IF NOT EXISTS collection_array_indexes (sourceTable TEXT, keypath TEXT, indexTable TEXT);`);
    const arrayIndexes = await this.store.database.allAsync("SELECT * from collection_array_indexes WHERE sourceTable = ?", this.name);
    for (let index of arrayIndexes) {
      this.arrayIndexes.set(index.keypath, index.indexTable);
    }
  }

  async initialize() {
    if (this.initializedStatus == "uninitialized") {
      this.initializedStatus = "initializing";
      await this.store.database.runAsync(`CREATE TABLE IF NOT EXISTS ${this.name} (_id TEXT PRIMARY KEY, document JSON);`);
      await this.refreshArrayIndexes();
      this.initializedStatus = "initialized";
    }
  }

  async createIndex(spec: {[key: string] : number}) {
    const name = this.name + "_" + Object.keys(spec).join("_");

    let sql = `CREATE INDEX ${name} on ${this.name}(`;

    sql = sql + Object.keys(spec).map((key: string) => {
      const order:number = spec[key];
      return "json_extract(document, '$." + key + "') " + (order ? "DESC" : "ASC");
    }).join(", ");
    
    sql = sql + ");"
    await this.store.database.runAsync(sql);
  }

  async ensureArrayIndex(key: string) {
    if (this.arrayIndexes.has(key)) {
      return;
    }
    await this.store.database.runAsync('BEGIN TRANSACTION');
    const tableName = `${this.name}_${key}`;
    await this.store.database.runAsync(`CREATE TABLE '${tableName}' AS SELECT _id, json_each.* from ${this.name}, json_each(document, '$.${key}')`);

    await this.store.database.runAsync(`DROP TRIGGER IF EXISTS ${tableName}_insert_trigger`);
    let sql = `CREATE TRIGGER '${tableName}_insert_trigger' AFTER INSERT ON ${this.name}
                                          BEGIN
                                            INSERT INTO ${tableName} SELECT NEW._id, json_each.* from json_each(NEW.document, '$.${key}'), ${this.name};
                                          END;`;
    await this.store.database.runAsync(sql);

    await this.store.database.runAsync(`DROP TRIGGER IF EXISTS ${tableName}_update_trigger`);
    sql = `CREATE TRIGGER '${tableName}_update_trigger' AFTER UPDATE ON ${this.name}
                                          BEGIN
                                            DELETE FROM ${tableName} WHERE _id = OLD._id;
                                            INSERT INTO ${tableName} SELECT NEW._id, json_each.* from json_each(NEW.document, '$.${key}'), ${this.name};
                                          END;`;
    await this.store.database.runAsync(sql);

    await this.store.database.runAsync(`DROP TRIGGER IF EXISTS ${tableName}_delete`);
    sql = `CREATE TRIGGER '${tableName}_delete' AFTER UPDATE ON ${this.name}
                                          BEGIN
                                            DELETE FROM ${tableName} WHERE _id = OLD._id;
                                          END;`;
    await this.store.database.runAsync(sql);

    //TODO update delete
    await this.store.database.runAsync('INSERT INTO collection_array_indexes VALUES (?, ?, ?)', [this.name, key, tableName]);
    await this.store.database.runAsync('COMMIT');
    this.arrayIndexes.set(key, tableName);
  }

  private parseComponent(component: any) : ([string, any] | Query)[] {

    if (Array.isArray(component)) {
      return component.map(x => new Query(this.parseComponent(x)));
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

  private parseKeyValue(key: string, value: any) : ([string, any] | Query) {
    if (key.startsWith('$')) {
      if (key.toLowerCase() == '$and') {
        return new Query(this.parseComponent(value), "AND");
      } else if (key.toLowerCase() == '$or') {
        return new Query(this.parseComponent(value), "OR");
      } else if (key.toLowerCase() == '$not') {
        return new Query(this.parseComponent(value));
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
          const q = new Query([[`${table}.value IN (${qMarks})`, value['$in']]]);
          q.otherTable = `INNER JOIN ${table} ON ${table}._id = ${this.name}._id`;
          //console.log(q);
          return q;

        } else { //unindexed

          const identifier = 'foo'; //TODO 
          const tableFunc = `json_each(document, '$.${key}') AS ${identifier}`;

          const qMarks = "?, ".repeat(value['$in'].length -1) + "?";
          const q = new Query([[`${identifier}.value IN (${qMarks})`, value['$in']]]);
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

  async find(q?: any, limit?: number): Promise<any[]> {
    let docs:any[];

    if (q && Object.keys(q).length > 0) {
      const query = new Query(this.parseComponent(q));
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT DISTINCT ${this.name}._id, ${this.name}.document from ${this.name} ${joins} WHERE ${whereSQL} ${(limit == undefined) ? "" : "LIMIT " + limit}`;
      //console.log(sql, args);
      docs = await this.store.database.allAsync(sql, args);
    } else {
      docs = await this.store.database.allAsync(`SELECT * from ${this.name}`);
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

  async insert(doc: any) {
    if (!doc._id) {
      doc._id = uuid.v4();
    }
    await this.store.database.runAsync(`INSERT INTO ${this.name} VALUES (?, ?);`, doc._id, JSON.stringify(doc, (key, value) => {
      if (key === '_id') {
        return undefined;
      } else {
        return value;
      }
    }));
  }

  async count(q: any): Promise<number> {
    if (q && Object.keys(q).length > 0) {
      const query = new Query(this.parseComponent(q));
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT COUNT(*) from ${this.name} ${joins} WHERE ${whereSQL}`;
      //console.log(sql, args);
      const doc:any = await this.store.database.getAsync(sql, args);
      return doc['COUNT(*)'];

    } else {
      const doc:any = await this.store.database.getAsync(`SELECT COUNT(*) from ${this.name}`);
      return doc['COUNT(*)'];
    }
  }

  async parseUpdateComponent(update: any, db: Database): Promise<[string, any[]]> {

    let updateSQL = 'document'
    let values: any[] = [];
    if (update['$inc']) {
      Object.keys(update['$inc']).forEach(k => {
        updateSQL = `json_set(document, '$.${k}', json_extract(${updateSQL}, '$.${k}') + ?)`;
        const val = update['$inc'][k];
        if (typeof val == 'string' || typeof val == 'number'){ 
          values.push(val);
        } else {
          values.push(JSON.stringify(val));
        }
      });
    }
    if (update['$set']) {
      Object.keys(update['$set']).forEach(k => {
        updateSQL = `json_set(${updateSQL}, '$.${k}', ?)`;
        const val = update['$set'][k];
        if (typeof val == 'string' || typeof val == 'number'){ 
          values.push(val);
        } else {
          values.push(JSON.stringify(val));
        }
      });
    }
    if (update['$push']) {
//INSERT INTO people SELECT "FOO", json_object('foo', json_group_array(DISTINCT value)) as myJson from (SELECT DISTINCT jsonToday.value FROM people, json_each(document, '$.foo') as jsonToday UNION SELECT 7 as jsonToday)
//json_set(document, '$.foo', json_group_array(DISTINCT value)) from (SELECT jsonToday.value FROM people, json_each(document, '$.foo') as jsonToday UNION SELECT 23 as jsonToday UNION SELECT 24 as jsonToday));

      Object.keys(update['$push']).forEach(k => {
        updateSQL = `(SELECT json_set(${updateSQL}, '$.${k}', json_group_array(value)) from (SELECT value FROM ${this.name}, json_each(${updateSQL}, '$.${k}'))`;
        //ugh, we just forked the updateSQL string:
        values = values.concat(values);

        const val = update['$push'][k];
        if (typeof val == 'string' || typeof val == 'number'){ 
          updateSQL = updateSQL + ` UNION SELECT ? `;
          values.push(val);
        } else if (Array.isArray(val)){ 
          for (let v of val) {
            updateSQL = updateSQL + ` UNION SELECT ? `;
            values.push(v);
          }
        } else {
          updateSQL = updateSQL + ` UNION SELECT ? `;
          values.push(JSON.stringify(val));
        }
        updateSQL = updateSQL + ")";
      });
    }

    if (update['$addToSet']) {
      throw new Error('$addToSet unimplemented yet');
    }

    if (values.length == 0 && update['$set'] == undefined && update['$inc'] == undefined) {
      Object.keys(update).forEach(k => {
        updateSQL = `json_set(${updateSQL}, '$.${k}', ?)`;
        const val = update[k];
        if (typeof val == 'string' || typeof val == 'number'){ 
          values.push(val);
        } else {
          values.push(JSON.stringify(val));
        }
      });
    }
    return [updateSQL, values];
  }

  async update(q: any, update: any) {

    const query = new Query(this.parseComponent(q));
    const db = await this.beginTransaction();

    if (query.join().length > 0) {
      throw new Error("Complex queries not yet implemented for updates");
    }

    try {
      const keys = new Set<string>();
      if (update['$inc']) {
        for (let k of Object.keys(update['$inc'])) {
          const updateSQL = `UPDATE ${this.name} SET document = json_set(document, '$.${k}', json_extract(document, '$.${k}') + ?) WHERE ${query.toString()}`;
          const args = query.values();
          const val = update['$inc'][k];
          if (typeof val != 'number'){ 
            throw new Error("Can't increment by non-number type: " + k + " += " + val);
          }
          args.unshift(val);
          //console.log(updateSQL, args);
          await db.runAsync(updateSQL, args);
          keys.add(k);
        }
      }

      if (update['$set']) {
        for (let k of Object.keys(update['$set'])) {

          if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }

          const updateSQL = `UPDATE ${this.name} SET document = json_set(document, '$.${k}', ?) WHERE ${query.toString()}`;
          const args = query.values();
          let val = update['$set'][k];

          if (typeof val != 'number' && typeof val != 'string'){ 
            val = JSON.stringify(val);
          }

          args.unshift(val);
          //console.log(updateSQL, args);
          await db.runAsync(updateSQL, args);
          keys.add(k);
        }
      }
      if (update['$push']) {
        for (let k of Object.keys(update['$push'])) {
          if (keys.has(k)) { throw new Error("Can't apply multiple updates to single key: " +  k); }
          // I worked out that you can set the array element at (0-based) index n for n elements to append an element:

         //UPDATE people SET document = json_set(document, '$.hobbies[' || json_array_length(json_extract(document, '$.hobbies')) || ']', "Skateboarding")  WHERE json_extract(document, "$.firstname") = "Bart";
          const updateSQL = `UPDATE ${this.name} SET document = json_set(document, '$.${k}[' || json_array_length(json_extract(document, '$.${k}')) || ']', ?)  WHERE ${query.toString()}`;
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

          //console.log(updateSQL, args);
          await db.runAsync(updateSQL, args);

          keys.add(k);
        }
      }
 
    } catch (error) {
      await this.rollback(db);
      throw error;
    }

 //   const limit = (spec || {}).multi ? '' : 'LIMIT 1'
    await this.commit(db);
    return;
  }
}


