const parse = require('mongo-parse').parse;

function getJSONPath(sqlColumn: string, jsonPath: string): string {
  return `json_extract(${sqlColumn}, "$.${jsonPath}")`;
}

export type Operator =  '$and' | '$or' | '$not' | '$in' | '$eq' | '$gt' | '$gte' | '$lt' | '$lte' | '$nin' | '$ne';
const operatorMap = {
  '$eq': 'IS',
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<=',
  '$ne': '!=',
}

function littoJSON(jsonPath: string) { return getJSONPath('documents', jsonPath); }

function operatorFor(o: Operator) {
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

function partToString(p: QueryPart): [string, any[]] {
  const values:any[] = []; 
  let res: Array<[string, Array<any>]>;
  let array: any[]; 
  let sql: string | undefined = undefined;
  switch(p.operator) {
    case '$and': 
      res = p.parts.map(partToString);
      array = ([] as Array<any>).concat(...res.map(x => x[1]));
    return [res.map(x=>x[0]).join(" AND "), array];
    case '$or': 
      res = p.parts.map(partToString);
      array = ([] as Array<any>).concat(...res.map(x => x[1]));
    return [res.map(x=>x[0]).join(" OR "), array];
    case '$not': 
      if (!sql) {sql = " IS NOT ";}
    case '$nin': 
      if (!sql) {sql = " NOT IN ";}
    case '$in': 
      if (!sql) {sql = " IN ";}
      if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
      sql = sql + formatOperand(p.operand);
      sql = littoJSON(p.field as string) + sql;
      return [sql, filterOperand(p.operand)];
    case '$gt':
      case '$gte':
      case '$lt':
      case '$lte':
      case '$ne':
      case '$eq':
      if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
    return [`${littoJSON(p.field as string)} ${operatorFor(p.operator)} ?`, filterOperand(p.operand)];
    case undefined:
      if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
    values.push(p.operand);
    return [`${littoJSON(p.field as string)} ${operatorFor('$eq')} ?`, filterOperand(p.operand)];
    default:
      throw new Error("TODO" + p);
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

export class NewQuery {
  parsedMongo: QueryObject;
  _results: Array<[string, Array<any>]>;
  constructor(q: any) {
    this.parsedMongo = parse(q);
  }

  get results() {
    if (!this._results) {
      this._results = this.parsedMongo.parts.map(partToString);
    }
    return this._results;
  }

  get sql(): string {
    return this.results.map(x => x[0]).join(' AND ');
  }

  values(): any[] {
    return ([] as Array<any>).concat(...this.results.map((x: [string, any[]]) => x[1]));
  }
}
