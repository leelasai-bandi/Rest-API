
const express = require('express');
const router = express.Router();
const dbService = require('../services/dbServices');
const logger = require('winston');

router.get('/', async (req, res, next) => {
  try {
    const tables = await dbService.getTables();
    res.json({
      success: true,
      data: tables,
    });
  } catch (err) {
    next(err);
  }
});

router.get('/:table', async (req, res, next) => {
  const { table } = req.params;
  const { columns, filter, limit, offset, order_by } = req.query;

  try {
    const result = await dbService.fetchData(table, {
      columns,
      filter,
      limit,
      offset,
      orderBy: order_by,
    });
    res.json({
      success: true,
      data: result.data,
      count: result.count,
    });
  } catch (err) {
    next(err);
  }
});

router.post('/:table', async (req, res, next) => {
  const { table } = req.params;
  const data = req.body;

  if (!data || Object.keys(data).length === 0) {
    return res.status(400).json({
      success: false,
      error: 'Request body cannot be empty',
    });
  }

  try {
    const result = await dbService.insertData(table, data);
    res.status(201).json({
      success: true,
      data: result.data,
      message: result.message,
    });
  } catch (err) {
    next(err);
  }
});

module.exports = router;



