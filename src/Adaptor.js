/** @module Adaptor */
import { setAuth, setUrl } from './Utils';
import 'regenerator-runtime/runtime.js';
import {
  execute as commonExecute,
  expandReferences,
  composeNextState,
} from 'language-common';
import fs from 'fs';
import parse from 'csv-parse';
import AdmZip from 'adm-zip';
import request from 'request';
import { BigQuery } from '@google-cloud/bigquery';

/**
 * Execute a sequence of operations.
 * Wraps `language-common/execute`, and prepends initial state for http.
 * @example
 * execute(
 *   create('foo'),
 *   delete('bar')
 * )(state)
 * @function
 * @param {Operations} operations - Operations to be performed.
 * @returns {Operation}
 */
export function execute(...operations) {
  const initialState = {
    references: [],
    data: null,
  };

  return state => {
    return commonExecute(...operations)({ ...initialState, ...state });
  };
}

export function fetch(url, output) {
  return state => {
    /* Create an empty file where we can save data */
    let file = fs.createWriteStream(output);
    /* Using Promises so that we can use the ASYNC AWAIT syntax */
    return new Promise((resolve, reject) => {
      let stream = request({
        /* Here you should specify the exact link to the file you are trying to download */
        uri: url,
        // headers: {
        //   Accept:
        //     'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        //   'Accept-Encoding': 'gzip, deflate, br',
        //   'Accept-Language':
        //     'en-US,en;q=0.9,fr;q=0.8,ro;q=0.7,ru;q=0.6,la;q=0.5,pt;q=0.4,de;q=0.3',
        //   'Cache-Control': 'max-age=0',
        //   Connection: 'keep-alive',
        //   'Upgrade-Insecure-Requests': '1',
        //   'User-Agent':
        //     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
        // },
        /* GZIP true for most of the websites now, disable it if you don't need it */
        // gzip: true,
      })
        .pipe(file)
        .on('finish', () => {
          console.log(`The file is finished downloading.`);
          resolve();
        })
        .on('error', error => {
          reject(error);
        });
    }).catch(error => {
      console.log(`Something happened: ${error}`);
    });
  };
}

export function unzip(input, output) {
  // something that unzips from a CSV and allows the output to be used for hte
  // input of `load(data, options)`

  return state => {
    return new Promise((resolve, reject) => {
      const zip = new AdmZip(input);
      const zipEntries = zip.getEntries();
      console.log(zipEntries.length);
      zip.extractAllTo(output, true);
    }).then(() => {
      console.log('extracted!');
      return state;
    });
  };
}

export function load(
  fileName,
  projectId,
  datasetId,
  tableId,
  loadOptions,
  callback
) {
  // something that loads data (from a CSV?) to BigQuery
  return state => {
    const bigquery = new BigQuery({
      credentials: state.configuration,
      projectId,
    });
    // In this example, the existing table contains only the 'Name', 'Age',
    // & 'Weight' columns. 'REQUIRED' fields cannot  be added to an existing
    // schema, so the additional column must be 'NULLABLE'.
    async function loadData() {
      // Retrieve destination table reference
      const [table] = await bigquery.dataset(datasetId).table(tableId).get();

      const destinationTableRef = table.metadata.tableReference;

      // Set load job options
      const options = { ...loadOptions, destinationTableRef };

      // Load data from a local file into the table
      const [job] = await bigquery
        .dataset(datasetId)
        .table(tableId)
        .load(fileName, options);

      console.log(`Job ${job.id} completed.`);
      console.log('New Schema:');
      console.log(job.configuration.load.schema.fields);

      // Check the job's status for errors
      const errors = job.status.errors;
      if (errors && errors.length > 0) {
        throw errors;
      }

      return state;
    }
    return loadData();
  };
}

/**
 * CSV-Parse for CSV conversion to JSON
 * @public
 * @example
 *  parseCSV("/home/user/someData.csv", {
 * 	  quoteChar: '"',
 * 	  header: false,
 * 	});
 * @function
 * @param {String} target - string or local file with CSV data
 * @param {Object} config - csv-parse config object
 * @returns {Operation}
 */
export function parseCSV(target, config) {
  return state => {
    return new Promise((resolve, reject) => {
      var csvData = [];

      try {
        fs.readFileSync(target);
        fs.createReadStream(target)
          .pipe(parse(config))
          .on('data', csvrow => {
            console.log(csvrow);
            csvData.push(csvrow);
          })
          .on('end', () => {
            console.log(csvData);
            resolve(composeNextState(state, csvData));
          });
      } catch (err) {
        var csvString;
        if (typeof target === 'string') {
          csvString = target;
        } else {
          csvString = expandReferences(target)(state);
        }
        csvData = parse(csvString, config, (err, output) => {
          console.log(output);
          resolve(composeNextState(state, output));
        });
      }
    });
  };
}

exports.fs = fs;

export {
  alterState,
  dataPath,
  dataValue,
  each,
  field,
  fields,
  lastReferenceValue,
  merge,
  sourceValue,
} from 'language-common';
