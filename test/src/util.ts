import { filterClauses, containsClauses } from '../../dist/util';
import { test } from 'ava';


test("filterClauses", async (t) => {
  t.deepEqual(
    filterClauses({
      foo:'bar', 
      '$push': {x:1},
      array: ['$a','$b','c'],
      x: {
        '$nested': 1
      }}),
      {
        foo: 'bar',
        array: ['$a', '$b', 'c'],
        x: {}
      }
    ,
    'filterClauses works'
    );

});

test("containsClauses", async (t) => {
  t.is(containsClauses({'$inc': 1}), true);
  t.is(containsClauses({foo :{'$bar': 1}}), true);
  t.is(containsClauses({foo :{'bar': 1}}), false);
});


