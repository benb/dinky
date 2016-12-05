import { NewQuery } from '../../';
import { test } from 'ava';

function getQuery(q: any) {
  return new NewQuery(q, "people", new Map<string, string>());
}


test.only("Query", async t => { 
  let q = getQuery({$and: [{foo: 1}, {bar: 2}]});
  console.log(q.sql);
  q = getQuery({foo: {'$ne': 1}});
  console.log(q.sql);
  q = getQuery({$and:[{"lname":"Ford"},{"marks.english": {$gt:35}}]});
  console.log(q.sql);
  q = getQuery({"ticket_no" : {"$nin" : [725, 542, 390]}});
  console.log(q.sql);
  q = getQuery({"ticket_no" : {"$in" : [725, 542, 390]}});
  console.log(q.sql);
  q = getQuery({"ticket_no" : null});
  console.log(q.sql);
  q = getQuery({"ticket_no" : {'$not':  null}});
  console.log(q.sql);

  const tableMap = new Map<string, string>();
  tableMap.set("ticket_no", "people_ticket_no");
  q = new NewQuery({"ticket_no" : {"$in" : [725, 542, 390]}}, "people", tableMap);
  console.log(q.sql);
  console.log(q.join);

});

