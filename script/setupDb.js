'use strict'
require('dotenv').config({ path: '../.env' })
const knex = require('knex')
const Table = require('../src/partitioning')

const dbName = process.env.MYSQL_DB || 'partition_test'

const connection = knex({
  client: 'mysql',
  connection: {
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || 3306,
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
  },
  pool: {
    min: 1,
    max: 10
  }
})

connection.raw(`CREATE SCHEMA IF NOT EXISTS ${dbName}`)
  .then(() => {
    console.log('Test db created successfully')
    process.exit(0)
  })
  .catch((err) => {
    throw err
  })
