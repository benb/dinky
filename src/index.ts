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
}

export class Collection {
  store: Store;
  name: string;
  initialized: boolean;

  async insertMany(data: any[]) {
  }



  constructor(store: Store, name: string) {
    this.store = store;
    this.name = name;
  }

  async initialize() {
    if (!this.initialized) {
      await this.store.database.runAsync(`CREATE TABLE IF NOT EXISTS ${this.name} (id TEXT PRIMARY KEY, document JSON);`);
      this.initialized = true;
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

  async createArrayIndex(field: string) {
  //  this.store.database
      /*
         CREATE TRIGGER people_favourite_colours_trigger AFTER INSERT ON people¬
           BEGIN¬
           INSERT INTO people_favourite_colours SELECT NEW.id, json_each.value from json_each(NEW.document, "$.colours");¬
         END;¬*/
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
      queries.push([`id IS ?`, component['_id']]);
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
        //todo allow array queries
        throw new Error("Unsupported query " + value);
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
      docs = await this.store.database.allAsync(`SELECT * from ${this.name} WHERE ${whereSQL}`, args);
    } else {
      docs = await this.store.database.allAsync(`SELECT * from ${this.name}`);
    }

    return docs.map(doc => {
      const parsed = JSON.parse(doc.document);
      parsed._id = doc.id;
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


    console.log(`UPDATE ${this.name} SET document = ${updateSQL} ${whereString}`, allValues);
    await this.store.database.runAsync(`UPDATE ${this.name} SET document = ${updateSQL} ${whereString}`, allValues);

  }
}


