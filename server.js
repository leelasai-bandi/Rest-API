const express = require('express');
const winston = require('winston');
require('dotenv').config();
const apiRoutes = require('./routes/api');
const errorHandler = require('./middleware/errorHandler');

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: `${process.env.LOG_DIR}/app.log` }),
    new winston.transports.Console(),
  ],
});

const app = express();//instillize 
app.use(express.json());//formate json

app.use('/api', apiRoutes);

app.use(errorHandler);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`);
});