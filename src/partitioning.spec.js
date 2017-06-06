'use strict'
const connection = require('../connection')
const test = require('tape')
const dedent = require('dedent')
const Table = require('./partitioning')
const sinon = require('sinon')

test('getPartitionStrings', function (t) {
  const sandbox = sinon.sandbox.create()
  const expected = dedent`
  PARTITION \`from20110125\` VALUES LESS THAN (TO_DAYS('2011-01-26')),
  PARTITION \`from20110126\` VALUES LESS THAN (TO_DAYS('2011-01-27')),
  PARTITION \`from20110127\` VALUES LESS THAN (TO_DAYS('2011-01-28')),
  PARTITION \`from20110128\` VALUES LESS THAN (TO_DAYS('2011-01-29')),
  PARTITION \`from20110129\` VALUES LESS THAN (TO_DAYS('2011-01-30')),
  PARTITION \`from20110130\` VALUES LESS THAN (TO_DAYS('2011-01-31')),
  PARTITION \`from20110131\` VALUES LESS THAN (TO_DAYS('2011-02-01')),`

  sandbox.useFakeTimers(new Date('2011-01-31').getTime())

  const result = Table.getPartitionStrings()
  t.equal(result, expected)
  sandbox.restore()
  t.end()
})

test('getPartitionsThatShouldExist', function (t) {
  t.plan(1)
  const sandbox = sinon.sandbox.create()
  const clock = sandbox.useFakeTimers()
  clock.tick(new Date('2017-04-16').getTime())
  const currentPartitions = [
    'from20170412',
    'from20170413',
    'from20170414'
  ]

  const dataRetention = 5
  const result = Table.getPartitionsThatShouldExist(dataRetention, currentPartitions)
  t.deepEqual(result, [
    { name: 'from20170412', description: 736797 },
    { name: 'from20170413', description: 736798 },
    { name: 'from20170414', description: 736799 },
    { name: 'from20170415', description: 736800 },
    { name: 'from20170416', description: 736801 }
  ])
  sandbox.restore()
  t.end()
})

test('cleanup', function (t) {
  connection.destroy()
  t.end()
})
