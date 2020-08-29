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

export function fetch(uri, output) {
  return state => {
    /* Create an empty file where we can save data */
    let file = fs.createWriteStream(output);
    /* Using Promises so that we can use the ASYNC AWAIT syntax */
    return new Promise((resolve, reject) => {
      let stream = request({ uri })
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
      console.log(`Unzipping ${zipEntries.length} file(s).`);
      zip.extractAllTo(output, true);
    }).then(() => {
      console.log(`Extracted all to ${output}`);
      return state;
    });
  };
}

export function load(
  dirPath,
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
    async function loadData(fileName) {
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

    return new Promise((resolve, reject) => {
      fs.readdir(dirPath, function (err, files) {
        //handling error
        if (err) {
          return console.log('Unable to scan directory: ' + err);
        }
        //listing all files using forEach
        files.forEach(function (file) {
          console.log(file);
          // Do whatever you want to do with the file
          return loadData(`${dirPath}/${file}`);
        });
      });
    }).then(() => {
      console.log('all done');
      return state;
    });
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
