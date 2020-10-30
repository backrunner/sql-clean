# SQL Clean

A simple script to clean useless data in database.

## Usage

Clone this repo and enter into the directory.

```bash
git clone -b main https://github.com/backrunner/sql-clean.git --depth 1
```

Install dependencies.

```bash
npm install
```

Create a file named "config.js" in the directory.

Template:

```javascript
export default {
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: '',
  database: 'demo',
  tables: [
    {
      name: 'log',
      timeCol: 'createTime',  // DATETIME type col
      mode: 'delete', // or drop
    }
  ],
  maxPersistDays: 7,  // max day data peresisted.
  cron: '*0 0 * * *',  // clean at 0:00 every day
};
```

Run the following command directly:

```bash
npm run start
```

## License

MIT
