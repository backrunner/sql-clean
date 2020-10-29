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
if (!config.tables || !Array.isArray(config.tables)) {
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

const doDelete = async (connection, table) => {
  const { name, timeCol: col } = table;
  const time = moment().subtract(config.maxPersistDays, 'day').format('YYYY-MM-DD') + ' 00:00:00';
  const sql = `DELETE FROM ${name} WHERE ${col} < '${time}'`;
  logger.debug(`[SQL] ${sql}`);
  try {
    await connection.execute(sql);
    logger.info(`Delete action for table [${name}] done.`);
  } catch (err) {
    logger.error(`Execute SQL for table [${name}] failed.`);
    logger.error(err);
  }
}

const doDrop = async (connection, table) => {
  const { name, timeCol: col } = table;
  const tempTableName = `${name}_${Math.floor(Math.random() * 10000)}`;
  const time = moment().subtract(config.maxPersistDays, 'day').format('YYYY-MM-DD') + ' 00:00:00';
  try {
    const createSql = `CREATE TABLE ${tempTableName} LIKE ${name}`;
    logger.debug(`[SQL] ${createSql}`);
    await connection.execute(createSql);
    logger.info(`Temp table for ${name} is created`);

    const insertSql = `INSERT INTO ${tempTableName} SELECT * FROM ${name} WHERE ${col} >= '${time}'`;
    logger.debug(`[SQL] ${insertSql}`);
    await connection.execute(insertSql);
    logger.info(`Data has copied to temp table [${tempTableName}]`);

    const dropSql = `DROP TABLE ${name}`;
    logger.debug(`[SQL] ${dropSql}`);
    await connection.execute(dropSql);
    logger.info(`Old table ${name} has been drop.`);

    const renameSql = `RENAME TABLE ${tempTableName} TO ${name}`;
    logger.debug(`[SQL] ${renameSql}`);
    await connection.execute(renameSql);
    logger.info(`Temp table renamed.`);
  } catch (err) {
    logger.error(`Execute SQL for table [${name}] failed.`);
    logger.error(err);
  }
}

cron.schedule(config.cron, async () => {
  logger.info('Clean task started.');
  const connection = await mysql.createConnection(mysqlOptions);
  const { tables } = config;
  for (let table of tables) {
    const { mode } = table;
    switch(mode) {
      case 'delete':
        doDelete(connection, table);
        break;
      case 'drop':
        doDrop(connection, table);
        break;
      default:
        logger.warn('Wrong delete mode, treat as delete.');
        doDelete(connection, table);
        break;
    }
  }
});

logger.info('SQL Clean service is running.');