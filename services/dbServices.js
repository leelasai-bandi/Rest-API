
const { pool } = require('../config/database');
const Promise = require('bluebird');
const logger = require('winston');

class DbService {
  async getTables() {
    const query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public';
    `;
    try {
      const res = await pool.query(query);
      return res.rows.map(row => row.table_name);
    } catch (err) {
      logger.error(`Error fetching tables: ${err.message}`);
      throw err;
    }
  }

  async getTableColumns(tableName) {
    const query = `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_schema = 'public' AND table_name = $1;
    `;
    try {
      const res = await pool.query(query, [tableName]);
      return res.rows.map(row => row.column_name);
    } catch (err) {
      logger.error(`Error fetching columns for table ${tableName}: ${err.message}`);
      throw err;
    }
  }

  async fetchData(tableName, params = {}) {
    const {
      columns = '*',
      filter = '',
      limit = 1000,
      offset = 0,
      orderBy = '',
    } = params;

    const tables = await this.getTables();
    if (!tables.includes(tableName)) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const validColumns = await this.getTableColumns(tableName);
    const requestedColumns = columns === '*' ? validColumns : columns.split(',').map(c => c.trim());
    const invalidColumns = requestedColumns.filter(c => !validColumns.includes(c));
    if (invalidColumns.length > 0) {
      throw new Error(`Invalid columns: ${invalidColumns.join(', ')}`);
    }

    let query = `SELECT ${requestedColumns.join(', ')} FROM ${tableName}`;
    const values = [];

    if (filter) {
      query += ` WHERE ${filter}`;
    }

    if (orderBy) {
      query += ` ORDER BY ${orderBy}`;
    }

    if (limit) {
      query += ` LIMIT $${values.length + 1}`;
      values.push(limit);
    }

    if (offset) {
      query += ` OFFSET $${values.length + 1}`;
      values.push(offset);
    }

    try {
      logger.info(`Executing query: ${query} with values: ${values}`);
      const res = await pool.query(query, values);
      return {
        data: res.rows,
        count: res.rowCount,
      };
    } catch (err) {
      logger.error(`Error executing query on table ${tableName}: ${err.message}`);
      throw err;
    }
  }

  async insertData(tableName, data) {
    const tables = await this.getTables();
    if (!tables.includes(tableName)) {
      throw new Error(`Table ${tableName} does not exist`);
    }

    const validColumns = await this.getTableColumns(tableName);
    const dataKeys = Object.keys(data);
    const invalidKeys = dataKeys.filter(key => !validColumns.includes(key));
    if (invalidKeys.length > 0) {
      throw new Error(`Invalid columns in data: ${invalidKeys.join(', ')}`);
    }

    const columns = dataKeys.join(', ');
    const placeholders = dataKeys.map((_, i) => `$${i + 1}`).join(', ');
    const values = dataKeys.map(key => data[key]);
    const query = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders}) RETURNING *`;

    try {
      logger.info(`Executing insert query: ${query} with values: ${values}`);
      const res = await pool.query(query, values);
      return {
        data: res.rows[0],
        message: 'Record inserted successfully',
      };
    } catch (err) {
      logger.error(`Error inserting into table ${tableName}: ${err.message}`);
      throw err;
    }
  }
}

module.exports = new DbService();