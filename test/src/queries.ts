import { Query } from '../../';
import { test } from 'ava';

function getQuery(q: any) {
  return new Query(q, "people", new Map<string, string>());
}


test("Query", async t => { 
  let q = getQuery({$and: [{foo: 1}, {bar: 2}]});
  q = getQuery({foo: {'$ne': 1}});
  q = getQuery({$and:[{"lname":"Ford"},{"marks.english": {$gt:35}}]});
  q = getQuery({"ticket_no" : {"$nin" : [725, 542, 390]}});
  q = getQuery({"ticket_no" : {"$in" : [725, 542, 390]}});
  q = getQuery({"ticket_no" : null});

  const tableMap = new Map<string, string>();
  tableMap.set("ticket_no", "people_ticket_no");
  q = new Query({"ticket_no" : {"$in" : [725, 542, 390]}}, "people", tableMap);

});

