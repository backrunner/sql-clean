import cron from 'node-cron';
import mysql from 'mysql2/promise';
import moment from 'moment';
import logger from './logger';
import config from './config';

// 校验cron
if (!cron.validate(config.cron)) {
  logger.error('Cron string is not valid');
  process.exit(-1);
}

// 校验tables
if (!tables || !Array.isArray(tables)) {
  logger.error('Tables option is not valid');
  process.exit(-1);
}

const mysqlOptions = {
  host: config.host,
  port: config.port,
  user: config.user,
  password: config.password,
  database: config.database,
};

const doDelete = async (connection) => {
  for (let table of tables) {
    const { name, timeCol: col } = table;
    const time = moment().subtract(config.maxPersistDays, 'day').format('YYYY-MM-DD') + ' 00:00:00';
    const sql = `DELETE FROM ${name} WHERE ${col} < '${time}'`;
    try {
      await connection.exeucte(sql);
      logger.info(`Delete action for table [${name}] done.`);
    } catch (err) {
      logger.error(`Execute SQL for table [${name}] failed.`);
      logger.error(err);
    }
  }
}

const doDrop = async (connection) => {
  for (let table of tables) {
    const { name, timeCol: col } = table;
    const tempTableName = `${name}_${Math.floor(Math.random * 10000)}`;
    const time = moment().subtract(config.maxPersistDays, 'day').format('YYYY-MM-DD') + ' 00:00:00';
    try {
      await connection.exeucte(`CREATE TABLE ${tempTableName} LIKE ${name}`);
      logger.info(`Temp table for ${name} is created`);
      await connection.exeucte(`INSERT INTO ${tempTableName} SELECT * FROM ${name} WHERE ${col} >= '${time}`);
      logger.info(`Data has copied to temp table [${tempTableName}]`);
      await connection.exeucte(`DROP TABLE ${name}`);
      logger.info(`Old table ${name} has been drop.`);
      await connection.exeucte(`RENAME TABLE ${tempTableName} TO ${name}`);
      logger.info(`Temp table renamed.`);
    } catch (err) {
      logger.error(`Execute SQL for table [${name}] failed.`);
      logger.error(err);
    }
  }
}

cron.schedule(config.cron, async () => {
  logger.info('Clean task started.');
  const connection = await mysql.createConnection(mysqlOptions);
  switch (config.deleteMode) {
    case 'delete':
      doDelete(connection);
      break;
    case 'drop':
      doDrop(connection);
      break;
    default:
      logger.warn('Wrong delete mode, treat as delete.');
      doDelete(connection);
      break;
  }
});