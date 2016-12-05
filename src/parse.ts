const parse = require('mongo-parse').parse;

function getJSONPath(sqlColumn: string, jsonPath: string): string {
  return `json_extract(${sqlColumn}, "$.${jsonPath}")`;
}

export type Operator =  '$and' | '$or' | '$not' | '$in' | '$eq' | '$gt' | '$gte' | '$lt' | '$lte' | '$nin' | '$ne' | '$not' | '$nin' | '$in';
const operatorMap = {
  '$eq': 'IS',
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<=',
  '$ne': '!=',
  '$not': "NOT",
  '$nin': "NOT IN", 
  '$in': "IN"
}

function littoJSON(jsonPath: string) { return getJSONPath('documents', jsonPath); }

function formatOperator(o: Operator) {
  return (operatorMap as any)[o];
}

function formatOperand(operand: any): any {
  if (Array.isArray(operand)) {
    return "(" + operand.map(formatOperand).join(", ") + ")";
  }
  if (!operand) {
    return "NULL";
  }
  return "?";
}

function filterOperand(operand: any): any {
  if (Array.isArray(operand)) {
    return operand.map(filterOperand);
  }
  if (operand == null) {
    return "NULL";
  } else {
    return operand;
  }
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

export class NewQuery {
  parsedMongo: QueryObject;
  arrayIndexes: Map<string, string>;
  name: string;
  private _results: QueryResult[];

  private partToString(p: QueryPart): QueryResult {
    switch(p.operator) {
      case '$and':  {
        const res = p.parts.map(x => this.partToString(x));
        const operands = ([] as Array<any>).concat(...res.map(x => x.operands));
        const sql = res.map(x=>x.sql).join(" AND ");
        return {sql, operands};
      }
      case '$or': { 
        const res = p.parts.map(x => this.partToString(x));
        const operands = ([] as Array<any>).concat(...res.map(x => x.operands));
        const sql = res.map(x => x.sql).join(" OR ");
        return {sql, operands};
      }
      case '$not': 
      case '$nin': 
      case '$in':  {
        if (!p.field) {throw new Error("Strange output in parsed query, expected field: " + p);}
        if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
        let sql:string = littoJSON(p.field as string) + " " + formatOperator(p.operator) + " " + formatOperand(p.operand);
        const operands = filterOperand(p.operand);
        let join = ""

        const table = this.arrayIndexes.get(p.field);
        if (table) {
          join = `INNER JOIN "${table}" ON "${table}"._id = "${this.name}"._id`;
          sql = `"${table}".values ${formatOperator(p.operator)} ${formatOperand(p.operand)}`;
        }

        return {sql, operands, join};
      }
      case '$gt':
      case '$gte':
      case '$lt':
      case '$lte':
      case '$ne':
      case '$eq': {
        if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
        const sql = `${littoJSON(p.field as string)} ${formatOperator(p.operator)} ?`;
        const operands = filterOperand(p.operand);
        return {sql, operands};
      }
      case undefined: {
        if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
        const sql = `${littoJSON(p.field as string)} ${formatOperator('$eq')} ?`;
        const operands = filterOperand(p.operand);
        return {sql, operands};
      }
      default:
        throw new Error("TODO" + p);
    }
  }



  constructor(q: any, name: string, arrayIndexes: Map<string, string>) {
    this.parsedMongo = parse(q);
    this.name = name;
    this.arrayIndexes = arrayIndexes;
  }

  private get results() {
    if (!this._results) {
      this._results = this.parsedMongo.parts.map(x => this.partToString(x));
    }
    return this._results;
  }

  get sql(): string {
    return this.results.map(x => x.sql).join(' AND ');
  }

  get values(): any[] {
    return ([] as Array<any>).concat(...this.results.map((x: QueryResult) => x.operands));
  }

  get join(): string {
    return this.results.map(x => x.join).join(', ');
  }
}
