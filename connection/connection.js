'use strict'
require('dotenv').config({ path: '../.env' })
const knex = require('knex')

const connection = knex({
  client: 'mysql',
  connection: {
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || 3306,
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DB || 'partition_test',
    multipleStatements: true
  },
  pool: {
    min: 1,
    max: 10
  }
})

module.exports = connection

