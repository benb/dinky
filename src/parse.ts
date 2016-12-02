const parser = require('mongo-parse');

function getJSONPath(sqlColumn: string, jsonPath: string) {
  return `json_extract(${sqlColumn}, "$.${jsonPath}")`;
}

type Operator =  '$and' | '$or' | '$not' | '$in' | '$eq' | '$gt' | '$gte' | '$lt' | '$lte' | '$nin' | '$ne';
const operatorMap = {
  '$eq': 'IS',
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<=',
  '$ne': '!=',
}

const littoJSON = getJSONPath.bind('documents');

function operatorFor(o: Operator) {
  return operatorMap[o];   
}

function partToString(p: QueryPart): [string, any[]] {
  const values = []; 
  let res:[string, any[]][];
  switch(p.operator) {
    case '$and': 
      res = p.parts.map(partToString);
    return [res.map(x=>x[0]).join(" AND "), [].concat(...res.map(x=>x[1]))];
    case '$or': 
      res = p.parts.map(partToString);
    return [res.map(x=>x[0]).join(" OR "), [].concat(...res.map(x=>x[1]))];
    case '$not': 
      res = p.parts.map(partToString);
    return [" IS NOT " + res.map(x=>x[0]).join(" AND "), [].concat(...res.map(x=>x[1]))];
    case '$gt':
      case '$gte':
      case '$lt':
      case '$lte':
      case '$ne':
      case '$eq':
      if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
    values.push(p.operand);
    return [`${littoJSON(p.field)} ${operatorFor(p.operator)} ?`, values];
    case undefined:
      if (p.parts.length > 0) {throw new Error("Unsupported query part " + p)};
    values.push(p.operand);
    return [`${littoJSON(p.field)} ${operatorFor('$eq')} ?`, values];
    default:
      throw new Error("TODO");
  }
}

declare class QueryObject {
  parts: QueryPart[];
}

class QueryPart {
  field?: string;
  operator?: Operator;
  operand?: any;
  parts?: QueryPart[];
  implicitField?: boolean;
}

class Query {
  parsedMongo: QueryObject;
  constructor(q: any) {
    this.parsedMongo = parser(q);
  }

  toString(): string {
    return this.parsedMongo.parts.map(partToString).join(' AND ');
  }

  values(): any[] {
    return [];
  }
}
