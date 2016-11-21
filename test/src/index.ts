import { Store } from '../../';
import { test } from 'ava';

async function basicDatabase() {
  const store = new Store();
  await store.open(':memory:');
  const people = await store.getCollection('people');
  await people.insert({firstname: "Maggie", lastname: "Simpson"});
  await people.insert({firstname: "Bart", lastname: "Simpson"});
  await people.insert({firstname: "Marge", lastname: "Simpson"});
  await people.insert({firstname: "Homer", lastname: "Simpson"});
  await people.insert({firstname: "Lisa", lastname: "Simpson"});
  await people.insert({firstname: "Lisa", lastname: "Kudrow"});
  return store;
}

test("Basic insertion and retrieval", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');

  const allPeople:any[] = await people.find({});

  t.is(allPeople.length, 6, "Six results");
  t.is(allPeople[0].firstname, "Maggie", "Correct firstname");

  await store.close();
});

test("Indexing", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');

  const badPlan = await store.database.allAsync("EXPLAIN QUERY PLAN SELECT * from people ORDER BY json_extract(document, '$.firstname')");
  t.falsy((badPlan[0].detail as string).indexOf("USING INDEX") > -1, "Can't use index until created");

  await people.createIndex({firstname: 1, lastname: 1});

  const plan = await store.database.allAsync("EXPLAIN QUERY PLAN SELECT * from people ORDER BY json_extract(document, '$.firstname')");

  t.is(plan.length, 1, "One row to plan");
  t.truthy((plan[0].detail as string).indexOf("USING INDEX") > -1, "Should use index");
  await store.close();
});

test("Queries", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');
  await people.createIndex({firstname: 1});
  await people.createIndex({lastname: 1});

  const barts = await people.find({firstname: "Bart"});
  t.is(barts.length, 1, "There's only one Bart");

  const simpsons = await people.find({lastname: "Simpson"});
  t.is(simpsons.length, 5, "There's five Simpsons");

  const lisas = await people.find({firstname: "Lisa"});
  t.is(lisas.length, 2, "Two Lisas");

  const lisaSimpsons = await people.find({firstname: "Lisa", lastname: "Simpson"});
  t.is(lisas.length, 2, "Only one Lisa Simpson");

});
