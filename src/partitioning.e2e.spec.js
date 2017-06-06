'use strict'
const test = require('tape')
const sinon = require('sinon')
const _ = require('lodash')
const moment = require('moment')
const connection = require('../connection')
const Table = require('./partitioning')
const accountId = 'accountId'

const expectedPartitionsBefore = [
  { partition_description: '0', partition_name: 'start' },
  { partition_description: '734139', partition_name: 'from20100101' },
  { partition_description: '734140', partition_name: 'from20100102' },
  { partition_description: '734141', partition_name: 'from20100103' },
  { partition_description: '734142', partition_name: 'from20100104' },
  { partition_description: '734143', partition_name: 'from20100105' },
  { partition_description: '734144', partition_name: 'from20100106' },
  { partition_description: '734145', partition_name: 'from20100107' },
  { partition_description: 'MAXVALUE', partition_name: 'future' }
]

const expectedPartitionsAfter = [
  { partition_description: '0', partition_name: 'start' },
  { partition_description: '734140', partition_name: 'from20100102' },
  { partition_description: '734141', partition_name: 'from20100103' },
  { partition_description: '734142', partition_name: 'from20100104' },
  { partition_description: '734143', partition_name: 'from20100105' },
  { partition_description: '734144', partition_name: 'from20100106' },
  { partition_description: '734145', partition_name: 'from20100107' },
  { partition_description: '734146', partition_name: 'from20100108' },
  { partition_description: 'MAXVALUE', partition_name: 'future' }
]

function getPartitions () {
  return connection('information_schema.partitions')
    .select('partition_name', 'partition_description')
    .where('table_schema', Table.dbName)
}

test('repartition', function (t) {
  const sandbox = sinon.sandbox.create()
  const clock = sandbox.useFakeTimers()
  const testDate = '2010-01-07'
  clock.tick(new Date(testDate).getTime())

  const dataToDelete = {
    data: 'delete it',
    created_at:  moment(testDate).subtract(6, 'day').toDate()
  }

  const dataToKeep = _.range(0, 6).map((day, idx) => {
    return {
      data: `keep it ${idx}`,
      created_at: moment(testDate).subtract(day, 'day').toDate()
    }
  })

  Table.create()
    .then(getPartitions)
    .then((names) => t.deepEqual(names, expectedPartitionsBefore))
    .then(() => Table.query().insert(dataToKeep.concat([ dataToDelete ])))
    .then(() => {
      clock.tick(24 * 60 * 60 * 1000 + 1)
      return Table.removeExpired(7)
    })
    .then(getPartitions)
    .then((names) => t.deepEqual(names, expectedPartitionsAfter))
    .then(() => Table.query().select('data', 'created_at'))
    .then((result) => {
      t.deepEqual(new Set(result), new Set(dataToKeep))
      sandbox.restore()
      return Table.drop()
    })
    .then(() => {
      t.end()
    })
    .catch((err) => {
      t.fail()
      console.error(err)
      sandbox.restore()
      return Table.drop()
    })
})

test('cleanup', function (t) {
  connection.destroy()
  t.end()
})
