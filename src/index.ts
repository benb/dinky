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
    console.log("EXECTING SQL", sql);
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

  parseComponent(component: any) {
    //Examples: 
    const queries = Object
      .keys(component)
      .filter(x => x != '_id')
      .map( (key) => {
        return this.parseKeyValue(key, component[key]);
      });
    if (component['_id']) {
      queries.push(['id = ?', component['_id']]);
    }
    return queries;
  }

  parseKeyValue(key: string, value: any) : [string, any] {
    if (key.startsWith('$')) {
      throw new Error("Unsupported query");
    } else {
      if (typeof value == 'string' || typeof value == 'number') {
        return [`json_extract(document, '$.${key}') = ?`, value];
      } else {
        throw new Error("Unsupported query");
      }
    }
  }


  async find(q?: any): Promise<any[]> {
    let docs:any[];
    let whereSQL:string[] = []; 
    let args:any[] = [];

    if (q) {
      let queriesValues = this.parseComponent(q);
      whereSQL = queriesValues.map(x => x[0]);
      args = queriesValues.map(x => x[1]);
    }
    if (whereSQL.length > 0) {
      console.log(`SELECT * from ${this.name} WHERE ${whereSQL.join(" AND ")}`, args);
      docs = await this.store.database.allAsync(`SELECT * from ${this.name} WHERE ${whereSQL.join(" AND ")}`, args);
    } else {
      docs = await this.store.database.allAsync(`SELECT * from ${this.name}`);
    }

    return docs.map(doc => {
      console.log(doc);
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
      console.log(key, value);
      if (key === '_id') {
        return undefined;
      } else {
        return value;
      }
    }));
  }
}
