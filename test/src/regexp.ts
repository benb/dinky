import * as sqlite from 'sqlite3';
import { Store } from '../../';
import { test } from 'ava';
import * as temp from 'temp';
import * as uuid from 'uuid';
import * as Rx from 'rxjs';

temp.track();

const awkwardString = "people-%.4";

async function tempDatabase(logging = false) {
  const store = new Store();
  await store.open(temp.mkdirSync() + "/temp.db", logging);
  return store;
}

async function basicDatabase(logging = false, idField = "_id") {
  const store = await tempDatabase(logging);

  await store.database.execAsync(`DROP TABLE IF EXISTS "${awkwardString}"`);
  const people = await store.getCollection(awkwardString, idField);
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

//this won't work for now 
//https://github.com/mapbox/node-sqlite3/issues/140
/*
test("regexp", async t => {
  const store = await basicDatabase();
  const people = await store.getCollection("people");

  const mPeople = await people.find({firstname: /^M/});
  t.is(mPeople.length, 2, "Correct count");
});
*/

test("like", async t => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);

  const mPeople = await people.find({firstname: {'$like': "M%"}});
  t.is(mPeople.length, 2, "Correct count");
  for (let person of mPeople) {
    t.truthy(person.firstname.startsWith("M"));
  }
});

test("not like", async t => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);

  const mPeople = await people.find({firstname: {'$not' : {'$like': "M%"}}});
  t.is(mPeople.length, 4, "Correct count");
  for (let person of mPeople) {
    t.falsy(person.firstname.startsWith("M"));
  }
});
