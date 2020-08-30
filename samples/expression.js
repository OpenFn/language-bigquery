alterState(state => {
  console.log(`Started ${state.year}/${state.month} at: ${Date()}`);
  return state;
});

fetch(
  'http://comtrade.un.org/api/get/bulk/C/M/' +
    state.year +
    state.month +
    '/ALL/HS?token=' +
    state.token,
  `/home/taylor/${state.year}${state.month}.zip`
);

unzip(
  `/home/taylor/${state.year}${state.month}.zip`,
  `/home/taylor/${state.year}${state.month}`
);

load(
  `/home/taylor/${state.year}${state.month}`,
  'value-chain-solutions', // project
  'test01', // dataset
  'fact_comtrade_2', // table
  {
    schema:
      'classification:STRING,year:STRING,period:STRING,period_desc:STRING,aggregate_level:STRING,is_leaf_code:STRING,trade_flow_code:STRING,trade_flow:STRING,reporter_code:STRING,reporter:STRING,reporter_iso:STRING,partner_code:STRING,partner:STRING,partner_iso:STRING,commodity_code:STRING,commodity:STRING,qty_unit_code:STRING,qty_unit:STRING,qty:INTEGER,netweight_kg:INTEGER,trade_value:INTEGER,flag:STRING',
    // schemaUpdateOptions: ['ALLOW_FIELD_ADDITION'],
    // createDisposition: 'CREATE_IF_NEEDED',
    writeDisposition: 'WRITE_APPEND',
    skipLeadingRows: 1,
  } // loadOptions
);

alterState(state => {
  console.log(
    `Upload done for ${state.year}/${state.month}, removing unzipped csv`
  );
  const dir = `/home/taylor/${state.year}${state.month}`;
  fs.rmdir(dir, { recursive: true }, err => {
    if (err) {
      throw err;
    }
    console.log(`${dir} is deleted!`);
  });
  console.log(`Finished ${state.year}/${state.month} at: ${Date()}`);
  return state;
});
