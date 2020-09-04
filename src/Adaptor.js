/** @module Adaptor */
import { setAuth, setUrl } from './Utils';
import 'regenerator-runtime/runtime.js';
import languageHttp from 'language-http';
import {
  execute as commonExecute,
  expandReferences,
  composeNextState,
} from 'language-common';
import fs from 'fs';
import https from 'https';
import parse from 'csv-parse';
import unzipper from 'unzipper';
import request from 'request';
import { parseStringPromise } from 'xml2js';
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

export function download(url, dest) {
  return state => {
    return new Promise((resolve, reject) => {
      console.log(url);
      console.log(dest);
      const file = fs.createWriteStream(dest, { flags: 'wx' });

      const request = https.get(url, response => {
        if (response.statusCode === 200) {
          response.pipe(file);
        } else {
          file.close();
          fs.unlink(dest, () => {}); // Delete temp file
          reject(
            `Server responded with ${response.statusCode}: ${response.statusMessage}.`
          );
        }
      });

      request.on('error', err => {
        file.close();
        fs.unlink(dest, () => {}); // Delete temp file
        reject(err.message);
      });

      file.on('finish', () => {
        resolve();
      });

      file.on('error', err => {
        file.close();

        if (err.code === 'EEXIST') {
          reject('File already exists');
        } else {
          fs.unlink(dest, () => {}); // Delete temp file
          reject(err.message);
        }
      });
    });
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
          resolve(state);
        })
        .on('error', error => {
          reject(error);
        });
    }).catch(error => {
      console.log(`Something happened: ${error}`);
    });
  };
}

// something that unzips from a CSV and allows the output to be used for hte
// input of `load(data, options)`
export function unzip(input, output) {
  return state => {
    console.log(`Unzipping ${input}`);
    return new Promise((resolve, reject) => {
      return fs
        .createReadStream(input)
        .pipe(unzipper.Extract({ path: output }))
        .on('finish', resolve);
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
    async function loadData(files) {
      // Retrieve destination table reference
      const [table] = await bigquery.dataset(datasetId).table(tableId).get();

      const destinationTableRef = table.metadata.tableReference;

      // Set load job options
      const options = { ...loadOptions, destinationTableRef };

      for (const file of files) {
        const fileName = `${dirPath}/${file}`;
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
      }

      return state;
    }

    return new Promise((resolve, reject) => {
      console.log('Google Big Query: loading files');
      return fs.readdir(dirPath, function (err, files) {
        //handling error
        if (err) {
          return console.log('Unable to scan directory: ' + err);
        }
        resolve(loadData(files));
      });
    }).then(() => {
      console.log('all done');
      return state;
    });
  };
}

export function parseXML(xml, options) {
  return state => {
    return parseStringPromise(xml, options).then(result => {
      console.log('Finished parsing. Result available in state.data');
      return composeNextState(state, result);
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

exports.languageHttp = languageHttp;

exports.fs = fs;

export {
  alterState,
  dataPath,
  combine,
  dataValue,
  each,
  field,
  fields,
  lastReferenceValue,
  merge,
  sourceValue,
} from 'language-common';
