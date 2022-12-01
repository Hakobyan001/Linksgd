// NPM Modules
import PSQL from '../config/variables.config';

const {
  PORT, HOST, DATABASE, USER, PASSWORD
} = PSQL.PSQL;

class UrlsModel {
  // // Methods
  static async getUrls(offset, limit) {
    const knex = require('knex')({
      client: 'pg',
      useNullAsDefault: true,
      connection: {
        port: PORT,
        host: HOST,
        database: DATABASE,
        user: USER,
        password: PASSWORD
      }
    });

    const urls = await knex
      .from('links')
      .select('domain', 'id')
      .orderBy('id')
      .offset(offset)
      .limit(limit);

    return urls;
  }
}

export default UrlsModel;
