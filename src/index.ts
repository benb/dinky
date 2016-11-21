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

  async createIndex(spec: any) {
    const name = this.name + "_" + Object.keys(spec).join("_");

    let sql = `CREATE INDEX ${name} on ${this.name}(`;

    sql = sql + Object.keys(spec).map(key => {
      return "json_extract(document, '$." + key + "') " + ((spec[key] < 0) ? "DESC" : "ASC");
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
    const sql = `CREATE TRIGGER '${tableName}_insert_trigger' AFTER INSERT ON ${this.name}
                                          BEGIN
                                            INSERT INTO ${tableName} SELECT NEW._id, json_each from json_each(NEW.document. '$.${key}');
                                          END;`;
    //console.log(sql);
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
      } else if (Array.isArray(value)) {
        //console.log(this.arrayIndexes, key);
        if (this.arrayIndexes.has(key)) { //indexed
          const table = this.arrayIndexes.get(key);
          const qMarks = "?, ".repeat(value.length -1) + "?";
          const q = new Query([[`${table}.value IN (${qMarks})`, value]]);
          q.otherTable = `INNER JOIN ${table} ON ${table}._id = ${this.name}._id`;
          //console.log(q);
          return q;
        } else { //unindexed
          const identifier = 'foo'; //TODO 
          const tableFunc = `json_each(document, '$.${key}') AS ${identifier}`;

          const qMarks = "?, ".repeat(value.length -1) + "?";
          const q = new Query([[`${identifier}.value IN (${qMarks})`, value]]);
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

  async find(q?: any): Promise<any[]> {
    let docs:any[];

    if (q && Object.keys(q).length > 0) {
      const query = new Query(this.parseComponent(q));
      const whereSQL = query.toString();
      const args = query.values();
      let joins = query.join().join(" ");
      if (joins) {
        joins = joins;
      }
      const sql = `SELECT DISTINCT ${this.name}._id, ${this.name}.document from ${this.name} ${joins} WHERE ${whereSQL}`;
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

  parseUpdateComponent(update: any): [string, any[]] {

    let updateSQL = 'document'
    const values: any[] = [];
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
    const [updateSQL, values] = this.parseUpdateComponent(update);
 //   const limit = (spec || {}).multi ? '' : 'LIMIT 1'
    let whereString = query.toString();
    if (whereString) {
      whereString = `WHERE ${whereString}`;
    }
    const allValues = values.concat(query.values());


    //console.log(`UPDATE ${this.name} SET document = ${updateSQL} ${whereString}`, allValues);
    await this.store.database.runAsync(`UPDATE ${this.name} SET document = ${updateSQL} ${whereString}`, allValues);

  }
}


