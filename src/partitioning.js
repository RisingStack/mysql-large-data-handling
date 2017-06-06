'use strict'
require('dotenv').config({ path: '../.env' })

const knex = require('../connection')
const dedent = require('dedent')
const moment = require('moment')
const _ = require('lodash')
const consts = require('../consts')

const dbName = process.env.MYSQL_DB || 'partition_test'

const tableName = 'test'
const Table = {
  dbName,
  tableName
}

Table.query = function () {
  return knex(tableName)
}

Table.create = function () {
  return knex.raw(dedent`
    CREATE TABLE IF NOT EXISTS \`${tableName}\` (
      \`id\` INTEGER NOT NULL AUTO_INCREMENT,
      \`data\` VARCHAR(255) NOT NULL,
      \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (\`id\`, \`created_at\`)
    )
    PARTITION BY RANGE ( TO_DAYS(\`created_at\`)) (
      PARTITION \`start\` VALUES LESS THAN (0),
      ${Table.getPartitionStrings()}
      PARTITION \`future\` VALUES LESS THAN MAXVALUE
    );
  `)
}

Table.getPartitionStrings = function () {
  const days = _.range(consts.MAX_DATA_RETENTION - 2, -2, -1)
  const partitions = days.map((day) => {
    const tomorrow = moment().subtract(day, 'day').format('YYYY-MM-DD')
    const today = moment().subtract(day + 1, 'day').format(consts.PARTITION_NAME_DATE_FORMAT)
    return `PARTITION \`from${today}\` VALUES LESS THAN (TO_DAYS('${tomorrow}')),`
  })
  return partitions.join('\n')
}

Table.drop = function () {
  return knex.raw(dedent`
    DROP TABLE IF EXISTS \`${tableName}\`
  `)
}

Table.clear = function () {
  return Table.drop()
    .then(() => Table.create())
}

Table.removeExpired = function (dataRetention) {
  return Table.getPartitions()
    .then((currentPartitions) => Table.repartition(dataRetention, currentPartitions))
}

Table.getPartitions = function () {
  return knex('information_schema.partitions')
    .select(knex.raw('partition_name as name'), knex.raw('partition_description as description')) // description holds the day of partition in mysql days
    .where('table_schema', dbName)
    .andWhere('partition_name', 'not in', [ 'start', 'future' ])
    .then((partitions) => partitions.map((partition) => ({
      name: partition.name,
      description: partition.description === 'MAX_VALUE' ? 'MAX_VALUE' : parseInt(partition.description)
    })))
}

Table.repartition = function (dataRetention, currentPartitions) {
  const partitionsThatShouldExist = Table.getPartitionsThatShouldExist(dataRetention, currentPartitions)

  const partitionsToBeCreated = _.differenceWith(partitionsThatShouldExist, currentPartitions, (a, b) => a.description === b.description)
  const partitionsToBeDropped = _.differenceWith(currentPartitions, partitionsThatShouldExist, (a, b) => a.description === b.description)

  const statement = dedent
    `${Table.reorganizeFuturePartition(partitionsToBeCreated)}
    ${Table.dropOldPartitions(partitionsToBeDropped)}`

  return knex.raw(statement)
}

Table.getPartitionsThatShouldExist = function (dataRetention, currentPartitions) {
  const days = _.range(dataRetention - 2, -2, -1)
  const oldestPartition = Math.min(...currentPartitions.map((partition) => partition.description))
  return days.map((day) => {
    const tomorrow = moment().subtract(day, 'day')
    const today = moment().subtract(day + 1, 'day')
    if (Table.getMysqlDay(today) < oldestPartition) {
      return null
    }

    return {
      name: `from${today.format(consts.PARTITION_NAME_DATE_FORMAT)}`,
      description: Table.getMysqlDay(tomorrow)
    }
  }).filter((partition) => !!partition)
}

Table.getMysqlDay = function (momentDate) {
  return momentDate.diff(moment([ 0, 0, 1 ]), 'days') // mysql dates are counted since 0 Jan 1 00:00:00
}

Table.reorganizeFuturePartition = function (partitionsToBeCreated) {
  if (!partitionsToBeCreated.length) return '' // there should be only one every day, and it is run hourly, so ideally 23 times a day it should be a noop
  const partitionsString = partitionsToBeCreated.map((partitionDescriptor) => {
    return `PARTITION \`${partitionDescriptor.name}\` VALUES LESS THAN (${partitionDescriptor.description}),`
  }).join('\n')

  return dedent`
    ALTER TABLE \`${tableName}\`
      REORGANIZE PARTITION future INTO (
        ${partitionsString}
        PARTITION \`future\` VALUES LESS THAN MAXVALUE
      );`
}

Table.dropOldPartitions = function (partitionsToBeDropped) {
  if (!partitionsToBeDropped.length) return ''
  let statement = `ALTER TABLE \`${tableName}\`\nDROP PARTITION\n`
  statement += partitionsToBeDropped.map((partition) => {
    return partition.name
  }).join(',\n')
  return statement + ';'
}

module.exports = Table
