import * as sqlite from 'sqlite3';
import { Store } from '../../';
import { test } from 'ava';
import * as temp from 'temp';
import * as uuid from 'uuid';

temp.track();

async function tempDatabase() {
  const store = new Store();
  await store.open(temp.mkdirSync() + "/temp.db");
  return store;
}
async function basicDatabase(logging = false) {
  const store = await tempDatabase();
  if (logging) {
    store.database.on('trace', console.log);
  }

  await store.database.execAsync('DROP TABLE IF EXISTS people');
  const people = await store.getCollection('people');
  await people.insertMany([
   {firstname: "Maggie", lastname: "Simpson", hobbies: ["dummies"]},
   {firstname: "Bart", lastname: "Simpson", hobbies: ["skateboarding", "boxcar racing", "annoying Homer"]},
   {firstname: "Marge", lastname: "Simpson"},
   {firstname: "Homer", lastname: "Simpson", hobbies: ["drinking", "gambling", "boxcar racing"]},
   {firstname: "Lisa", lastname: "Simpson", hobbies: ["tai chi", "chai tea", "annoying Homer"]},
   {firstname: "Lisa", lastname: "Kudrow"}
  ]);
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

  await people.ensureIndex({firstname: 1, lastname: 1});

  const plan = await store.database.allAsync("EXPLAIN QUERY PLAN SELECT * from people ORDER BY json_extract(document, '$.firstname')");

  t.is(plan.length, 1, "One row to plan");
  t.truthy((plan[0].detail as string).indexOf("USING INDEX") > -1, "Should use index");
  await store.close();
});

test("Queries", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');
  await people.ensureIndex({firstname: 1});
  await people.ensureIndex({lastname: 1});

  const barts = await people.find({firstname: "Bart"});
  t.is(barts.length, 1, "There's only one Bart");

  const simpsons = await people.find({lastname: "Simpson"});
  t.is(simpsons.length, 5, "There's five Simpsons");

  const lisas = await people.find({firstname: "Lisa"});
  t.is(lisas.length, 2, "Two Lisas");

  const lisaSimpsons = await people.find({firstname: "Lisa", lastname: "Simpson"});
  t.is(lisas.length, 2, "Only one Lisa Simpson");

  const lisasAndSimpsons = await people.find({'$or': [{firstname: "Lisa"}, {lastname: "Simpson"}]});
  t.is(lisasAndSimpsons.length, 6, "Five Simpsons and a Kudrow");

  const nonSimpsons = await people.find({lastname: {$not: "Simpson"}});
  t.is(nonSimpsons.length, 1, "One non-Simpson");
});

test("Updates", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');
  await people.ensureIndex({firstname: 1});
  await people.ensureIndex({lastname: 1});

  await people.update({firstname: "Lisa", lastname: "Simpson"}, {$set: {lastname: "Van Houten"} });
  await people.update({firstname: "Bart", lastname: "Simpson"}, {$set: {lastname: "Van Houten"} });

  const peeps = await people.find();

  t.is((await people.find({lastname: "Van Houten"})).length, 2, "Two Van Houtens");


});

test("Increment", async (t) => {
  const store = await tempDatabase();
  const people = await store.getCollection('people');

  await people.insert({firstname: "Lisa", lastname: "Simpson", age: 8});
  await people.insert({firstname: "Bart", lastname: "Simpson", age: 10});
  let bart = await people.findOne({firstname:"Bart"});
  let lisa = await people.findOne({firstname:"Lisa"});
  
  t.is(bart.age, 10, "Correct value in document");
  t.is(lisa.age, 8, "Correct value in document");

  await people.update({firstname: "Bart", lastname: "Simpson"}, {$inc: {age: 1} });
  bart = await people.findOne({firstname:"Bart"});

  t.is(bart.age, 11, "Correctly incremented");

  await people.update({firstname: "Bart", lastname: "Simpson"}, {$inc: {age: -10} });
  bart = await people.findOne({firstname:"Bart"});

  t.is(bart.age, 1, "Correctly incremented");

});

test("Arrays", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');

  await people.ensureArrayIndex('hobbies');
  t.truthy(people.arrayIndexes.has('hobbies'), 'indexed on hobbies');

  const homerAnnoyers = await people.find({hobbies: {'$in': ["annoying Homer"]}});
  t.is(homerAnnoyers.length, 2, "Correct result count");
  t.deepEqual(homerAnnoyers.map(x => x.firstname).sort(), ["Bart", "Lisa"], "Correct objects");

  const racersAndAnnoyers = await people.find({hobbies: {'$in' : ["annoying Homer", "boxcar racing"]}});
  t.is(racersAndAnnoyers.length, 3, "Correct result count");
  t.deepEqual(racersAndAnnoyers.map(x => x.firstname).sort(), ["Bart", "Homer", "Lisa"], "Correct objects");
});

test("Array Push", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection('people');

  await people.ensureArrayIndex('hobbies');
  t.truthy(people.arrayIndexes.has('hobbies'), 'indexed on hobbies');

  let homer: any;
  for (let x of ["TV", "Beer", "Go Crazy"]) {
    homer = await people.findOne({'firstname': 'Homer'});
    t.is(homer.hobbies.indexOf(x), -1, "Should not have hobby to start");
    await people.update({'firstname': 'Homer'}, {'$push': {'hobbies': x}});
    homer = await people.findOne({'firstname': 'Homer'});
    t.not(homer.hobbies.indexOf(x), -1, "Should add to array");
  }

  const tvWatcher = await people.findOne( {hobbies: {'$in' : ['TV'] } });
  t.is(tvWatcher.firstname, "Homer", "Can retrieve based on array query");
});
