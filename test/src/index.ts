import * as sqlite from 'sqlite3';
import { Store } from '../../';
import { test } from 'ava';
import * as temp from 'temp';
import * as uuid from 'uuid';
import * as Rx from '@reactivex/rxjs';

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

test("Basic insertion and retrieval", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);

  const allPeople:any[] = await people.find({});

  t.is(allPeople.length, 6, "Six results");

  await store.close();
});

test("Indexing", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);

  const badPlan = await store.database.allAsync(`EXPLAIN QUERY PLAN SELECT * from "${awkwardString}" ORDER BY json_extract(document, '$.firstname')`);
  t.falsy((badPlan[0].detail as string).indexOf("USING INDEX") > -1, "Can't use index until created");

  await people.ensureIndex({firstname: 1, lastname: 1});

  const plan = await store.database.allAsync(`EXPLAIN QUERY PLAN SELECT * from "${awkwardString}" ORDER BY json_extract(document, '$.firstname')`);

  t.is(plan.length, 1, "One row to plan");
  t.truthy((plan[0].detail as string).indexOf("USING INDEX") > -1, "Should use index");
  await store.close();
});

test("Unique Index", async (t) => {
  let store = await basicDatabase();
  let people = await store.getCollection(awkwardString);

  await people.ensureIndex({firstname: 1, lastname: 1}, {unique: true});
  t.plan(2);

  try {
    await people.insert({firstname: "Homer", lastname: "Simpson"});
  } catch(error) {
    t.truthy(true);
  }

  t.throws(people.insert({firstname: "Homer", lastname: "Simpson"}));

});

test("Queries", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  await people.ensureIndex({firstname: 1});
  await people.ensureIndex({lastname: 1});

  const barts = await people.find({firstname: "Bart"});
  t.is(barts.length, 1, "Correct object count");

  const simpsons = await people.find({lastname: "Simpson"});
  t.is(simpsons.length, 5, "There's five Simpsons");

  const lisas = await people.find({firstname: "Lisa"});
  t.is(lisas.length, 2, "Two Lisas");

  const lisaSimpsons = await people.find({firstname: "Lisa", lastname: "Simpson"});
  t.is(lisas.length, 2, "Only one Lisa Simpson");

  const lisasAndSimpsons = await people.find({'$or': [{firstname: "Lisa"}, {lastname: "Simpson"}]});
  t.is(lisasAndSimpsons.length, 6, "Five Simpsons and a Kudrow");

  const nonSimpsons = await people.find({lastname: {$ne: "Simpson"}});
  t.is(nonSimpsons.length, 1, "One non-Simpson");
});

test("Updates", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  await people.ensureIndex({firstname: 1});
  await people.ensureIndex({lastname: 1});

  await people.update({firstname: "Lisa", lastname: "Simpson"}, {$set: {lastname: "Van Houten"} });
  await people.update({firstname: "Bart", lastname: "Simpson"}, {$set: {lastname: "Van Houten"} });

  const peeps = await people.find();

  t.is((await people.find({lastname: "Van Houten"})).length, 2, "Two Van Houtens");


});

test("Increment", async (t) => {
  const store = await tempDatabase();
  const people = await store.getCollection(awkwardString);

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
  const people = await store.getCollection(awkwardString);

  await people.ensureArrayIndex('hobbies');
  t.truthy(people.arrayIndexes.has('hobbies'), 'indexed on hobbies');

  const homerAnnoyers = await people.find({hobbies: {'$in': ["annoying Homer"]}});
  t.is(homerAnnoyers.length, 2, "Correct result count");
  t.deepEqual(homerAnnoyers.map(x => x.firstname).sort(), ["Bart", "Lisa"], "Correct objects");

  const racersAndAnnoyers = await people.find({hobbies: {'$in' : ["annoying Homer", "boxcar racing"]}});
  t.is(racersAndAnnoyers.length, 3, "Correct result count");
  t.deepEqual(racersAndAnnoyers.map(x => x.firstname).sort(), ["Bart", "Homer", "Lisa"], "Correct objects");
});

test("Array $push and $pop", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);

//  await people.ensureArrayIndex('hobbies');
//  t.truthy(people.arrayIndexes.has('hobbies'), 'indexed on hobbies');

  let homer: any;
  for (let x of ["TV", "Beer", "Go Crazy"]) {
    homer = await people.findOne({'firstname': 'Homer'});
    t.is(homer.hobbies.indexOf(x), -1, "Should not have hobby to start");
    await people.update({'firstname': 'Homer'}, {'$push': {'hobbies': x}});
    homer = await people.findOne({'firstname': 'Homer'});
    t.not(homer.hobbies.indexOf(x), -1, "Should add to array");
  }

  const tvWatcher = await people.findOne( {hobbies: {'$in' : ['TV'] } });
  t.truthy(tvWatcher, "Can retrieve based on array query");
  t.is(tvWatcher.firstname, "Homer", "Can retrieve based on array query");

  const hobbies = tvWatcher.hobbies;
  await people.update({firstname: 'Homer'}, {'$pop': {'hobbies' : 1 } });
  homer = await people.findOne({firstname: 'Homer'})
  let dbHobbies = homer.hobbies;
  hobbies.pop();
  t.deepEqual(hobbies, dbHobbies, "Standard $pop works");


  await people.update({firstname: 'Homer'}, {'$pop': {'hobbies' : 1 } });
  dbHobbies = (await people.findOne({firstname: 'Homer'})).hobbies;
  hobbies.pop();
  t.deepEqual(hobbies, dbHobbies, "Standard $pop works");

  await people.update({firstname: 'Homer'}, {'$pop': {'hobbies' : -1 } });
  dbHobbies = (await people.findOne({firstname: 'Homer'})).hobbies;
  hobbies.shift();
  t.deepEqual(hobbies, dbHobbies, "Standard $pop works");

  await people.update({firstname: 'Homer'}, {'$pop': {'hobbies' : -1 } });
  dbHobbies = (await people.findOne({firstname: 'Homer'})).hobbies;
  hobbies.shift();
  t.deepEqual(hobbies, dbHobbies, "Standard $pop works");
});

test("$nin", async t => {
  for (let index of [true, false]) {
    const store = await basicDatabase();
    const people = await store.getCollection(awkwardString);
    if (index) { await people.ensureArrayIndex('hobbies'); }

    const nonSkateboarders = await people.find({'hobbies': {'$nin': ['skateboarding'] }});
    console.log(nonSkateboarders);
    const everyone = await people.find();
    t.is(nonSkateboarders.length + 1, everyone.length, "Only one skateboarder");
  }
});

test("Array $addToSet", async t => {
  const store = await basicDatabase(true);
  const people = await store.getCollection(awkwardString);
  let homer: any;

  for (let x of ["TV", "Beer", "Go Crazy"]) {
    homer = await people.findOne({'firstname': 'Homer'});
    t.is(homer.hobbies.indexOf(x), -1, "Should not have hobby to start");
    await people.update({'firstname': 'Homer'}, {'$addToSet': {'hobbies': x}});
    homer = await people.findOne({'firstname': 'Homer'});
    t.not(homer.hobbies.indexOf(x), -1, "Should add to array");
  }

  const hobbiesCount = homer.hobbies.length;

  for (let x of ["TV", "Beer", "Go Crazy"]) {
    await people.update({'firstname': 'Homer'}, {'$addToSet': {'hobbies': x}});
    homer = await people.findOne({'firstname': 'Homer'});
    t.is(homer.hobbies.length, hobbiesCount, `Should not add to array ${homer.hobbies}`);
  }

});

test("'complex' updates", async (t) => {
  for (let index of [true, false]) {
    const store = await basicDatabase();
    const people = await store.getCollection(awkwardString);
    if (index) { await people.ensureArrayIndex('hobbies'); }

    await people.update({hobbies : {'$in': ["boxcar racing"]}}, {'$push': {'hobbies' : 'TV'}}, {multi: true});
    const tvWatchers = await people.find({hobbies: {'$in': ['boxcar racing']} });
    t.is(tvWatchers.length, 2, "Correct number of entries updated");
    for (let person of tvWatchers) {
      t.not(person.hobbies.indexOf('TV'), -1, "Contains pushed entry");
    }
  }
});

test("upsert", async (t) => {
  for (let index of [true, false]) {
    const store = await basicDatabase();
    const people = await store.getCollection(awkwardString);
    if (index) {
      await people.ensureArrayIndex('hobbies');
    }
    await people.update({firstname: 'Ned', lastname: 'Flanders'}, {'$push': {'hobbies' : 'church'}}, {upsert: true});
    let ned = await people.findOne({hobbies : {'$in': ['church']}});
    t.truthy(ned, "Upsert created an object");
    t.is(ned.firstname, 'Ned', 'Upsert creates fields');

    await people.update({firstname: 'Ned', lastname: 'Flanders'}, {'$push': {'hobbies' : 'gardening'}}, {upsert: true});
    ned = await people.findOne({hobbies : {'$in': ['gardening']}});
    t.is(await people.count({firstname: 'Ned'}), 1, 'upsert doesn\'t insert unless necessary');
    t.is(ned.firstname, 'Ned', 'upsert does standard update to correct document');
  }
});

test("id field", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  const person = await people.findOne();
  const id = person._id;
  people.idField = "uuid";
  const samePerson = await people.findOne({uuid: id});
  t.is(samePerson.uuid, id, "Correct identifier");
  t.is(samePerson.firstname, person.firstname, "Correct fields");
  t.is(samePerson.lastname, person.lastname, "Correct fields");
});

test("update document", async(t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  const person = await people.findOne();
  person.firstname = "Fred";
  person.lastname = "Flintstone";
  await people.update({_id: person._id}, person);
  const newMan = await people.findOne({_id: person._id});
  t.is(newMan.firstname, person.firstname, "Update works");
  t.is(newMan.lastname, person.lastname, "Update works");
});

test("Basic Rx", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);

  t.plan(6);
  await new Promise( (resolve, reject) => {
    people.findObservable()
    .do({complete: resolve})
    .catch((err, _) => {reject(err); return Rx.Observable.of("ERROR");})
    .subscribe((item: any) => {
      t.truthy(item.firstname);
    });
  });

  await store.close();
});

test("Orderding", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  const allPeople:any[] = await people.find({$order: {firstname: 1}});

  const sortedPeople = allPeople.sort((a, b) => {
    if(a.firstname < b.firstname) return -1;
    if(a.firstname > b.firstname) return 1;
    return 0;
  });

  t.deepEqual(allPeople.map(x=>x.firstname), sortedPeople.map(x=>x.firstname), "Database should return ordered list for 1");

  const reversePeople = await people.find({$order: {firstname: -1}});
  t.deepEqual(reversePeople.map(x=>x.firstname), sortedPeople.reverse().map(x=>x.firstname), "Database should return ordered list for 1");

  const bothNamesOrdered = await people.findObservable().sort({firstname: 1, lastname: 1}).toArray().toPromise();
  const doubleSortedPeople= allPeople.sort((a, b) => {
    if(a.firstname < b.firstname) return -1;
    if(a.firstname > b.firstname) return 1;
    if(a.lastname < b.lastname) return -1;
    if(a.lastname > b.lastname) return 1;
    return 0;
  });
  t.deepEqual(doubleSortedPeople, bothNamesOrdered, "Should fully order output");

  const bothNamesOrderedReversed = await people.findObservable().sort({firstname: -1, lastname: -1}).limit(2).toArray().toPromise();
  t.deepEqual(doubleSortedPeople.reverse().splice(0, 2), bothNamesOrderedReversed, "Should fully order output");

});

test("DBCursor", async (t) => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  const twoPeople = await people.findObservable().limit(2).toArray().toPromise();
  t.is(twoPeople.length, 2, "limit() works");
});

test("Transactions", async(t) => {
  const store = await basicDatabase();
  await store.withinTransaction( async store => {
    const people = await store.getCollection(awkwardString);
    await people.insert({firstname: "Fred", lastname: "Flintstone"});
    return;
  });
  const people = await store.getCollection(awkwardString);
  const person = await people.findOne({firstname: "Fred"});
  t.is(person.lastname, "Flintstone", "Object created within transaction");
});

test("Custom ID", async (t) => {
  const store = await basicDatabase(false, "custom");
  const people = await store.getCollection(awkwardString);
  const person = await people.findOne();
  t.truthy(person.custom, "Should have an identifier");
  const samePerson = await people.findOne({"custom": person.custom});
  t.deepEqual(person, samePerson, "Lookup should work with custom ID");

  await people.update({"custom":"foo"}, {$set: {firstname: "Foo"}}, {upsert: true});
  const foo = await people.findOne({firstname: "Foo"});
  t.is(foo.custom, "foo", "Should use custom ID for upsert");
});

test("Deletion", async t => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  await people.delete({firstname: "Bart"});
  t.is(await people.count(), 5);
  await people.delete({firstname: "Homer"});
  t.is(await people.count(), 4);
  await people.delete({lastname: "Simpson"}, {justOne: true});
  t.is(await people.count(), 3);
  await people.delete({lastname: "Simpson"});
  t.is(await people.count(), 1);
});

test("boolean values", async t => {
  const store = await basicDatabase();
  const people = await store.getCollection(awkwardString);
  await people.update({'firstname': 'Lisa'}, {'$set': {deleted: true}}, {multi: true});
  const deletedCount = await people.count({'deleted': true });
  t.is(deletedCount, 2, "Count on boolean should work");
});

test("null", async t => {
  /*
     Test in mongo console:
> db.test.find();
{ "_id" : ObjectId("58458bda989b1c04b5fde61f"), "boolitem" : true }
{ "_id" : ObjectId("58458bdd989b1c04b5fde620"), "boolitem" : false }
{ "_id" : ObjectId("58458be7989b1c04b5fde621"), "something" : "foo" }
> db.test.find({'boolitem': false});
{ "_id" : ObjectId("58458bdd989b1c04b5fde620"), "boolitem" : false }
> db.test.find({'boolitem': true});
{ "_id" : ObjectId("58458bda989b1c04b5fde61f"), "boolitem" : true }
> db.test.find({'boolitem': null});
{ "_id" : ObjectId("58458be7989b1c04b5fde621"), "something" : "foo" }
*/
  const store = await tempDatabase();
  const test = await store.getCollection('test');
  for (let object of [{boolitem: false}, {boolitem: true}, {something: "foo"}]) {
    await test.insert(object);
  }

  const trues = (await test.find({boolitem: true})).map(x => {delete x._id; return x});
  const falses = (await test.find({boolitem: false})).map(x => {delete x._id; return x});
  const nulls = (await test.find({boolitem: null})).map(x => {delete x._id; return x});

  t.deepEqual(trues, [{boolitem: true}], "true lookup works");
  t.deepEqual(falses, [{boolitem: false}], "false lookup works");
  t.deepEqual(nulls, [{something: "foo"}], "null lookup works");
});

test("save", async t => {
  const store = await basicDatabase();
  const test = await store.getCollection("test");
  await test.save({_id: 'foo', other: 'bar'});
  let retrieval = await test.findOne({_id: 'foo'});
  t.is(retrieval.other, 'bar', "Should get back saved object");
  await test.save({_id: 'foo', other: 'bar2'});

  retrieval = await test.findOne({_id: 'foo'});
  t.is(retrieval.other, 'bar2', "Should get back updated object");

  await test.save({other: 'baz'});
  retrieval = await test.findOne({other: 'baz'});
  t.truthy(retrieval._id, "Should be inserted with an id");

});
