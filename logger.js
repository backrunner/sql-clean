import log4js from 'log4js';

log4js.configure({
  appenders: {
    toConsole: {
      type: 'console',
    },
    toFile: {
      type: 'dateFile',
      filename: './logs/runtime',
      pattern: 'yyyy-MM-dd.log',
      encoding: 'utf-8',
      alwaysIncludePattern: true,
    },
  },
  categories: {
    default: {
      appenders: ['toConsole', 'toFile'],
      level: 'info',
    },
  },
});

const logger = log4js.getLogger();

export default logger;