"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.execute = execute;
exports.fetch = fetch;
exports.unzip = unzip;
exports.load = load;
exports.parseCSV = parseCSV;
Object.defineProperty(exports, "alterState", {
  enumerable: true,
  get: function get() {
    return _languageCommon.alterState;
  }
});
Object.defineProperty(exports, "dataPath", {
  enumerable: true,
  get: function get() {
    return _languageCommon.dataPath;
  }
});
Object.defineProperty(exports, "dataValue", {
  enumerable: true,
  get: function get() {
    return _languageCommon.dataValue;
  }
});
Object.defineProperty(exports, "each", {
  enumerable: true,
  get: function get() {
    return _languageCommon.each;
  }
});
Object.defineProperty(exports, "field", {
  enumerable: true,
  get: function get() {
    return _languageCommon.field;
  }
});
Object.defineProperty(exports, "fields", {
  enumerable: true,
  get: function get() {
    return _languageCommon.fields;
  }
});
Object.defineProperty(exports, "lastReferenceValue", {
  enumerable: true,
  get: function get() {
    return _languageCommon.lastReferenceValue;
  }
});
Object.defineProperty(exports, "merge", {
  enumerable: true,
  get: function get() {
    return _languageCommon.merge;
  }
});
Object.defineProperty(exports, "sourceValue", {
  enumerable: true,
  get: function get() {
    return _languageCommon.sourceValue;
  }
});

var _Utils = require("./Utils");

require("regenerator-runtime/runtime.js");

var _languageCommon = require("language-common");

var _fs = _interopRequireDefault(require("fs"));

var _csvParse = _interopRequireDefault(require("csv-parse"));

var _admZip = _interopRequireDefault(require("adm-zip"));

var _request = _interopRequireDefault(require("request"));

var _bigquery = require("@google-cloud/bigquery");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { "default": obj }; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(_next, _throw); } }

function _asyncToGenerator(fn) { return function () { var self = this, args = arguments; return new Promise(function (resolve, reject) { var gen = fn.apply(self, args); function _next(value) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value); } function _throw(err) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err); } _next(undefined); }); }; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

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
function execute() {
  for (var _len = arguments.length, operations = new Array(_len), _key = 0; _key < _len; _key++) {
    operations[_key] = arguments[_key];
  }

  var initialState = {
    references: [],
    data: null
  };
  return function (state) {
    return _languageCommon.execute.apply(void 0, operations)(_objectSpread({}, initialState, {}, state));
  };
}

function fetch(uri, output) {
  return function (state) {
    /* Create an empty file where we can save data */
    var file = _fs["default"].createWriteStream(output);
    /* Using Promises so that we can use the ASYNC AWAIT syntax */


    return new Promise(function (resolve, reject) {
      var stream = (0, _request["default"])({
        uri: uri
      }).pipe(file).on('finish', function () {
        console.log("The file is finished downloading.");
        resolve();
      }).on('error', function (error) {
        reject(error);
      });
    })["catch"](function (error) {
      console.log("Something happened: ".concat(error));
    });
  };
}

function unzip(input, output) {
  // something that unzips from a CSV and allows the output to be used for hte
  // input of `load(data, options)`
  return function (state) {
    return new Promise(function (resolve, reject) {
      var zip = new _admZip["default"](input);
      var zipEntries = zip.getEntries();
      console.log("Unzipping ".concat(zipEntries.length, " file(s)."));
      zip.extractAllTo(output, true);
    }).then(function () {
      console.log("Extracted all to ".concat(output));
      return state;
    });
  };
}

function load(dirPath, projectId, datasetId, tableId, loadOptions, callback) {
  // something that loads data (from a CSV?) to BigQuery
  return function (state) {
    var bigquery = new _bigquery.BigQuery({
      credentials: state.configuration,
      projectId: projectId
    }); // In this example, the existing table contains only the 'Name', 'Age',
    // & 'Weight' columns. 'REQUIRED' fields cannot  be added to an existing
    // schema, so the additional column must be 'NULLABLE'.

    function loadData(_x) {
      return _loadData.apply(this, arguments);
    }

    function _loadData() {
      _loadData = _asyncToGenerator( /*#__PURE__*/regeneratorRuntime.mark(function _callee(fileName) {
        var _yield$bigquery$datas, _yield$bigquery$datas2, table, destinationTableRef, options, _yield$bigquery$datas3, _yield$bigquery$datas4, job, errors;

        return regeneratorRuntime.wrap(function _callee$(_context) {
          while (1) {
            switch (_context.prev = _context.next) {
              case 0:
                _context.next = 2;
                return bigquery.dataset(datasetId).table(tableId).get();

              case 2:
                _yield$bigquery$datas = _context.sent;
                _yield$bigquery$datas2 = _slicedToArray(_yield$bigquery$datas, 1);
                table = _yield$bigquery$datas2[0];
                destinationTableRef = table.metadata.tableReference; // Set load job options

                options = _objectSpread({}, loadOptions, {
                  destinationTableRef: destinationTableRef
                }); // Load data from a local file into the table

                _context.next = 9;
                return bigquery.dataset(datasetId).table(tableId).load(fileName, options);

              case 9:
                _yield$bigquery$datas3 = _context.sent;
                _yield$bigquery$datas4 = _slicedToArray(_yield$bigquery$datas3, 1);
                job = _yield$bigquery$datas4[0];
                console.log("Job ".concat(job.id, " completed."));
                console.log('New Schema:');
                console.log(job.configuration.load.schema.fields); // Check the job's status for errors

                errors = job.status.errors;

                if (!(errors && errors.length > 0)) {
                  _context.next = 18;
                  break;
                }

                throw errors;

              case 18:
                return _context.abrupt("return", state);

              case 19:
              case "end":
                return _context.stop();
            }
          }
        }, _callee);
      }));
      return _loadData.apply(this, arguments);
    }

    return new Promise(function (resolve, reject) {
      _fs["default"].readdir(dirPath, function (err, files) {
        //handling error
        if (err) {
          return console.log('Unable to scan directory: ' + err);
        } //listing all files using forEach


        files.forEach(function (file) {
          console.log(file); // Do whatever you want to do with the file

          return loadData("".concat(dirPath, "/").concat(file));
        });
      });
    }).then(function () {
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


function parseCSV(target, config) {
  return function (state) {
    return new Promise(function (resolve, reject) {
      var csvData = [];

      try {
        _fs["default"].readFileSync(target);

        _fs["default"].createReadStream(target).pipe((0, _csvParse["default"])(config)).on('data', function (csvrow) {
          console.log(csvrow);
          csvData.push(csvrow);
        }).on('end', function () {
          console.log(csvData);
          resolve((0, _languageCommon.composeNextState)(state, csvData));
        });
      } catch (err) {
        var csvString;

        if (typeof target === 'string') {
          csvString = target;
        } else {
          csvString = (0, _languageCommon.expandReferences)(target)(state);
        }

        csvData = (0, _csvParse["default"])(csvString, config, function (err, output) {
          console.log(output);
          resolve((0, _languageCommon.composeNextState)(state, output));
        });
      }
    });
  };
}

exports.fs = _fs["default"];
