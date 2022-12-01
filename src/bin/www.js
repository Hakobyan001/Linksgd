// Standard modules
import 'dotenv/config';
import 'regenerator-runtime';

// Modules from this project
import cluster from 'cluster';
import UrlService from '../services/UrlService';
import { UrlsModel } from '../models';
import db from '../../knex.config';

const knex = require('knex')(db.option);

const numCPUs = require('os').cpus().length;

let start = 0;
let end = 0;
const step = 20;
const worker = [];
const limit = 240;
let stop = 0;

async function isPrimary() {
  if (cluster.isPrimary) {
    const links = await UrlsModel.getUrls(0, limit);
    console.log(links.length);
    for (let i = 0; i < numCPUs; i += 1) {
      worker.push(cluster.fork());
      start = step * i;
      end = start + step;

      worker[i].send(links.slice(start, end));

      worker[i].on('message', async (msg) => {
        const rejectedData = await knex
          .from('links')
          .whereIn('id', msg.data[0])
          .update({ status: 'passive' });

        console.log('Table update rejected', rejectedData);

        const fulfilledData = await knex
          .from('links')
          .whereIn('id', msg.data[1])
          .update({ status: 'active' });

        console.log('Table update fulfilled', fulfilledData);
      });

      worker[i].on('error', (error) => {
        console.log(error);
      });
    }

    cluster.on('exit', async (currWorker) => {
      stop += step;
      start = end;
      end = start + step;
      const count = start + step - (step * numCPUs);
      console.log('Worker is died');

      const diedWorkerIndex = worker.findIndex((w) => w.id === currWorker.id);

      if (stop <= limit - step * numCPUs) {
        worker[diedWorkerIndex] = cluster.fork();
      }

      console.log(count, ' => Has been checked!');

      const chunk = links.slice(start, end);
      console.log('start, end => ', start, end);
      console.log('chunk => ', chunk);
      worker[diedWorkerIndex].send(chunk);

      worker[diedWorkerIndex].on('message', async (msg) => {
        console.log('msg on => ', msg);

        const rejectedData = await knex
          .from('links')
          .whereIn('id', msg.data[0])
          .update({ status: 'passive' });

        console.log('Table update rejected', rejectedData);

        const fulfilledData = await knex
          .from('links')
          .whereIn('id', msg.data[1])
          .update({ status: 'active' });

        console.log('Table update fulfilled', fulfilledData);
      });

      worker[diedWorkerIndex].on('error', (error) => {
        console.log(error);
      });
    });
  } else {
    process.on('message', async (msg) => {
      process.send({ data: await UrlService.checkUrls(msg) });
      process.kill(process.pid);
    });
  }
}

isPrimary();