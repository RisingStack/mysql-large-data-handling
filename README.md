# Handling large amount of data with MySQL and Node.js

Sources for the post on RisingStack engineering of the same title.

## npm scripts
Environmental variables can be provided by placing a `.env` file in the root or when invoking the scripts.

# used env vars
- `MYSQL_HOST` defaults to  `localhost`
- `MYSQL_PORT` defaults to `3306`,
- `MYSQL_USER` defaults to `root`,
- `MYSQL_PASSWORD`,
- `MYSQL_DB` defaults to `partition_test`,

### setup
Make sure you have MySQL installed and running
```bash
npm run setup
```

creates a schema defined by `MYSQL_DB`, used by the tests later on

### test
#### run all tests
```bash
npm test
```

#### run unit tests only
```bash
npm run test-unit
```

#### run integration tests only
Make sure MySQL is installed and running
```bash
npm run test-e2e
```

# Contributing
If you don't agree with anything or you found a mistake that needs to be corrected feel free to open an issue, leave a comment under the post or open a pull request.
