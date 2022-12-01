import PSQL from './src/config/variables.config';

const {
  PORT, HOST, DATABASE, USER, PASSWORD
} = PSQL.PSQL;

export default {
  development: {
    client: 'pg',
    useNullAsDefault: true,
    seeds: { directory: 'seeds/dev' },
    connection: {
      port: PORT,
      host: HOST,
      database: DATABASE,
      user: USER,
      password: PASSWORD
    }
  },

  option: {
    client: 'pg',
    useNullAsDefault: true,
    connection: {
      port: PORT,
      host: HOST,
      database: DATABASE,
      user: USER,
      password: PASSWORD
    }
  }
};
