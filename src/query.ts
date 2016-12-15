const parse = require('mongo-parse').parse;
import * as uuid from 'uuid';

function getJSONPath(sqlColumn: string, jsonPath: string): string {
  return `json_extract(${sqlColumn}, "$.${jsonPath}")`;
//  return `ifnull(json_extract(${sqlColumn}, "$.${jsonPath}"), "SOMETHING ELSE")`;
}

export type Operator =  '$and' | '$or' | '$not' | '$in' | '$eq' | '$gt' | '$gte' | '$lt' | '$lte' | '$nin' | '$ne' | '$not' | '$nin' | '$in' | '$like';
const operatorMap = {
  '$eq': 'IS',
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<=',
  '$ne': '!=',
  '$not': "NOT",
  '$nin': "NOT IN", 
  '$in': "IN",
  '$like': "LIKE"
}

function formatOperator(o?: Operator) {
  return (operatorMap as any)[o || '$eq'];
}

function formatOperand(operand: any): any {
  if (Array.isArray(operand)) {
    return "(" + operand.map(formatOperand).join(", ") + ")";
  }
  if (operand === null) {
    return "NULL";
  }
  return "?";
}

function filterOperand(operand: any): any {
  if (Array.isArray(operand)) {
    return operand.map(filterOperand).filter(x => {return x && (x!=[])});
  }
  if (operand === null) {
    return [];
  }
  return operand;
}

export declare class QueryObject {
  parts: QueryPart[];
}

export class QueryPart {
  field?: string;
  operator?: Operator;
  operand?: any;
  parts: QueryPart[];
  implicitField?: boolean;
}

class QueryResult {
  sql: string;
  operands: any[];
  join?: string;
}

export class Query {
  parsedMongo: QueryObject;
  arrayIndexes: Map<string, string>;
  name: string;
  nonJSONFields: {[field: string]: string};
  private _results: QueryResult[];

  littoJSON(jsonPath: string, operand: any = {}) { 
    if (this.nonJSONFields[jsonPath]) {
      return this.nonJSONFields[jsonPath];
    } else {
      return getJSONPath('document', jsonPath);
    }
  }

  private partToString(p: QueryPart, upstreamOperator: string = ""): QueryResult {
    switch(p.operator) {
      case '$and':  {
        const res = p.parts.map(x => this.partToString(x));
        const operands = ([] as Array<any>).concat(...res.map(x => x.operands, upstreamOperator));
        const sql = res.map(x=>x.sql).join(" AND ");
        return {sql, operands};
      }
      case '$or': { 
        const res = p.parts.map(x => this.partToString(x));
        const operands = ([] as Array<any>).concat(...res.map(x => x.operands, upstreamOperator));
        const sql = res.map(x => x.sql).join(" OR ");
        return {sql, operands};
      }
      case '$not': 
        const res = p.parts.map(part => this.partToString(part, formatOperator(p.operator)));
        const operands = ([] as Array<any>).concat(...res.map(x => x.operands, upstreamOperator));
        const sql = res.map(x=>x.sql).join(" AND ");
        return {sql, operands};
      case '$nin': 
      case '$in':  {
        if (!p.field) {throw new Error("Strange output in parsed query, expected field: " + p);}
        if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
        const operands = filterOperand(p.operand);
        const joinTable = uuid.v4();
        let join:string | undefined = `, json_each(${this.littoJSON(p.field as string, p.operand)}) as "${joinTable}"`;
        let sql:string = `"${joinTable}".value ${upstreamOperator} ${formatOperator('$in')} ${formatOperand(p.operand)}`;

        const table = this.arrayIndexes.get(p.field);
        if (table) {
          join = `INNER JOIN "${table}" ON "${table}"._id = "${this.name}"._id`;
          //operator is '$in' regardless
          //we handle '$nin' by inverting the matches, below
          sql = `"${table}".value ${upstreamOperator} ${formatOperator('$in')} ${formatOperand(p.operand)}`;
        }

        if (p.operator == '$nin') {
          // WHERE _id NOT IN ( SELECT _id FROM people, json_each(people.document, '$.a') WHERE json_each.value IS 4);
          sql = `"${this.name}"._id NOT IN ( SELECT "${this.name}"._id FROM "${this.name}" ${join} WHERE ${sql})`;
          join = undefined;
        }

        return {sql, operands, join};
      }
      case '$gt':
      case '$gte':
      case '$lt':
      case '$lte':
      case '$ne':
      case '$eq': 
      case '$like':
      case undefined: {
        if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
        let sql = `(${this.littoJSON(p.field as string)} ${upstreamOperator} ${formatOperator(p.operator || '$eq')} ?)`;
        const operands = filterOperand(p.operand);
        return {sql, operands};
      }
      default:
        throw new Error("TODO" + p);
    }
  }

  constructor(q: any, name: string, arrayIndexes: Map<string, string>, nonJSONFields:{[id: string]: string} = {_id: '_id'}) {
    this.parsedMongo = parse(q);
    this.name = name;
    this.arrayIndexes = arrayIndexes;
    this.nonJSONFields = nonJSONFields;
  }

  private get results() {
    if (!this._results) {
      this._results = this.parsedMongo.parts.map(x => this.partToString(x));
    }
    return this._results;
  }

  get sql(): string {
    const sql = this.results.map(x => x.sql).join(' AND ');
    return sql;
  }

  get values(): any[] {
    const v = ([] as Array<any>).concat(...this.results.map((x: QueryResult) => x.operands));
    return v;
  }

  get join(): string {
    const joins = this.results.map(x => x.join).filter(x => {return x && x.length > 0});
    if (joins.length > 0) {
      return joins.join(" ");
    }
    return "";
  }
}
